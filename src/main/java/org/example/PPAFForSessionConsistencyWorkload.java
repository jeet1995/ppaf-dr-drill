package org.example;

import com.azure.cosmos.ConnectionMode;
import com.azure.cosmos.CosmosAsyncClient;
import com.azure.cosmos.CosmosAsyncContainer;
import com.azure.cosmos.CosmosAsyncDatabase;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.implementation.CosmosDaemonThreadFactory;
import com.azure.cosmos.implementation.TestConfigurations;
import com.azure.cosmos.models.CosmosContainerProperties;
import com.azure.cosmos.models.ThroughputProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class PPAFForSessionConsistencyWorkload implements Workload {

    private static final Logger logger = LoggerFactory.getLogger(PPAFForSessionConsistencyWorkload.class);
    private static final List<String> CONFIGURED_SYSTEM_PROPERTIES = Arrays.asList(
            "COSMOS.IS_PER_PARTITION_AUTOMATIC_FAILOVER_ENABLED",
            "COSMOS.IS_SESSION_TOKEN_FALSE_PROGRESS_MERGE_ENABLED",
            "COSMOS.E2E_TIMEOUT_ERROR_HIT_THRESHOLD_FOR_PPAF",
            "COSMOS.E2E_TIMEOUT_ERROR_HIT_TIME_WINDOW_IN_SECONDS_FOR_PPAF",
            "COSMOS.STALE_PARTITION_UNAVAILABILITY_REFRESH_INTERVAL_IN_SECONDS",
            "COSMOS.ALLOWED_PARTITION_UNAVAILABILITY_DURATION_IN_SECONDS",
            "COSMOS.PARTITION_LEVEL_CIRCUIT_BREAKER_CONFIG" // Implicitly set when COSMOS.IS_PER_PARTITION_AUTOMATIC_FAILOVER_ENABLED is set to true
    );

    private static final Book DESIGNATED_BOOK = Book.build("1");

    // Cosmos DB SDK Requirements:
    //  1. Create should have E2E timeout of 5s
    //  2. Create should run until first availability error
    //  3. Reads should continue running
    //  4. Apply customer applied PPCB override and not implicit override through PPAF
    // Cosmos DB account Requirements
    //  1. Session Consistency Single-Region Multi-Write account
    //  2. Use a single-partition container - start with 3000 RUs
    @Override
    public void execute(Configuration cfg) {
        Object lock = new Object();

        AtomicInteger createSuccessCount = new AtomicInteger(0);
        AtomicInteger createFailureCount = new AtomicInteger(0);
        AtomicInteger readSuccessCount = new AtomicInteger(0);
        AtomicInteger readFailureCount = new AtomicInteger(0);
        AtomicBoolean isFailureDetectedOnCreate = new AtomicBoolean(false);
        AtomicReference<String> latestRecordedSessionTokenFromLatestCreate = new AtomicReference<>("");

        CopyOnWriteArrayList<String> successfullyPersistedIds = new CopyOnWriteArrayList<>();
        ThreadLocalRandom random = ThreadLocalRandom.current();

        Duration runDuration = cfg.getRunningTime();

        List<String> preferredRegions = Utils.getPreferredRegions(cfg);

        int parallelism = cfg.getNumberOfThreads();

        ScheduledThreadPoolExecutor scheduledThreadPoolExecutor = new ScheduledThreadPoolExecutor(2 * parallelism, new CosmosDaemonThreadFactory("CosmosCreateExecutor"));

        ScheduledFuture<?>[] scheduledFutures = new ScheduledFuture[2 * parallelism];

        String documentEndpoint = cfg.getAccountHost().isEmpty() ? TestConfigurations.HOST : cfg.getAccountHost();
        String masterKey = cfg.getAccountMasterKey().isEmpty() ? TestConfigurations.MASTER_KEY : cfg.getAccountMasterKey();
        String drillId = cfg.getDrillId();

        boolean shouldIncludeReadWorkload = cfg.shouldExecuteReadWorkload();

        ConnectionMode connectionMode = cfg.getConnectionMode();

        logger.info("Run Configurations : {}", cfg);

        CosmosAsyncClient cosmosAsyncClient = null;

        try {

            CosmosClientBuilder clientBuilder = new CosmosClientBuilder()
                    .endpoint(documentEndpoint)
                    .key(masterKey)
                    .preferredRegions(preferredRegions)
                    .userAgentSuffix(drillId)
                    .sessionRetryOptions(WorkloadUtils.LOCAL_REGION_PREFERRED_SESSION_RETRY_OPTIONS);

            if (connectionMode == ConnectionMode.DIRECT) {
                clientBuilder = clientBuilder.directMode();
            } else {
                clientBuilder = clientBuilder.gatewayMode();
            }

            System.setProperty("COSMOS.IS_PER_PARTITION_AUTOMATIC_FAILOVER_ENABLED", "true");
            System.setProperty("COSMOS.IS_SESSION_TOKEN_FALSE_PROGRESS_MERGE_ENABLED", "true");
            System.setProperty("COSMOS.E2E_TIMEOUT_ERROR_HIT_THRESHOLD_FOR_PPAF", "5");
            System.setProperty("COSMOS.E2E_TIMEOUT_ERROR_HIT_TIME_WINDOW_IN_SECONDS_FOR_PPAF", "120");
            System.setProperty("COSMOS.STALE_PARTITION_UNAVAILABILITY_REFRESH_INTERVAL_IN_SECONDS", "300");
            System.setProperty("COSMOS.ALLOWED_PARTITION_UNAVAILABILITY_DURATION_IN_SECONDS", "300");
            System.setProperty(
                    "COSMOS.PARTITION_LEVEL_CIRCUIT_BREAKER_CONFIG",
                    "{\"isPartitionLevelCircuitBreakerEnabled\": true, "
                            + "\"circuitBreakerType\": \"CONSECUTIVE_EXCEPTION_COUNT_BASED\","
                            + "\"consecutiveExceptionCountToleratedForReads\": 100,"
                            + "\"consecutiveExceptionCountToleratedForWrites\": 50,"
                            + "}");

            cosmosAsyncClient = clientBuilder.buildAsyncClient();

            cosmosAsyncClient
                    .createDatabaseIfNotExists(cfg.getDatabaseName())
                    .onErrorResume(throwable -> Mono.empty())
                    .block();

            CosmosAsyncDatabase cosmosAsyncDatabase = cosmosAsyncClient.getDatabase(cfg.getDatabaseName());

            CosmosContainerProperties cosmosContainerProperties = new CosmosContainerProperties(cfg.getContainerName(), cfg.getPartitionKeyPath());
            cosmosAsyncDatabase
                    .createContainerIfNotExists(cosmosContainerProperties, ThroughputProperties.createManualThroughput(cfg.getPhysicalPartitionCount() * 10_000))
                    .onErrorResume(throwable -> Mono.empty())
                    .block();

            CosmosAsyncContainer cosmosAsyncContainer = cosmosAsyncDatabase.getContainer(cfg.getContainerName());

            // Upsert 1 particular Book first
            cosmosAsyncContainer.upsertItem(DESIGNATED_BOOK).block();

            Instant startTime = Instant.now();

            for (int i = 0; i < scheduledFutures.length; i++) {

                final int finalI = i;

                if (i % 2 == 0) {
                    scheduledFutures[i] = scheduledThreadPoolExecutor.schedule(() -> {
                        try {
                            WorkloadUtils.onCreateStopOnFirstFailure(
                                    cosmosAsyncContainer,
                                    cfg,
                                    startTime,
                                    runDuration,
                                    finalI,
                                    createSuccessCount,
                                    createFailureCount,
                                    isFailureDetectedOnCreate,
                                    latestRecordedSessionTokenFromLatestCreate);
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    }, 10, TimeUnit.MILLISECONDS);
                } else {

                    if (shouldIncludeReadWorkload) {

                        scheduledFutures[i] = scheduledThreadPoolExecutor.schedule(() -> {
                            try {
                                WorkloadUtils.onSessionRead(
                                        cosmosAsyncContainer,
                                        cfg,
                                        startTime,
                                        runDuration,
                                        finalI,
                                        readSuccessCount,
                                        readFailureCount,
                                        "1",
                                        latestRecordedSessionTokenFromLatestCreate);
                            } catch (InterruptedException e) {
                                throw new RuntimeException(e);
                            }
                        }, 10, TimeUnit.MILLISECONDS);
                    }
                }
            }

            while (!Instant.now().minus(runDuration).isAfter(startTime)) {}

            logger.info("Workload complete!");

            for (ScheduledFuture<?> scheduledFuture : scheduledFutures) {
                scheduledFuture.cancel(true);
            }

        } finally {

            WorkloadUtils.cleanUpSystemProperties(CONFIGURED_SYSTEM_PROPERTIES);

            if (cosmosAsyncClient != null) {
                cosmosAsyncClient.close();
            }
        }
    }
}