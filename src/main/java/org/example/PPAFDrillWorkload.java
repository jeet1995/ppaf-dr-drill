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
import com.azure.cosmos.test.faultinjection.CosmosFaultInjectionHelper;
import com.azure.cosmos.test.faultinjection.FaultInjectionCondition;
import com.azure.cosmos.test.faultinjection.FaultInjectionConditionBuilder;
import com.azure.cosmos.test.faultinjection.FaultInjectionConnectionType;
import com.azure.cosmos.test.faultinjection.FaultInjectionOperationType;
import com.azure.cosmos.test.faultinjection.FaultInjectionResultBuilders;
import com.azure.cosmos.test.faultinjection.FaultInjectionRule;
import com.azure.cosmos.test.faultinjection.FaultInjectionRuleBuilder;
import com.azure.cosmos.test.faultinjection.FaultInjectionServerErrorResult;
import com.azure.cosmos.test.faultinjection.FaultInjectionServerErrorType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class PPAFDrillWorkload implements Workload {

    private static final Logger logger = LoggerFactory.getLogger(PPAFDrillWorkload.class);
    private static final List<String> CONFIGURED_SYSTEM_PROPERTIES = Arrays.asList(
            "COSMOS.IS_SESSION_TOKEN_FALSE_PROGRESS_MERGE_ENABLED",
            "COSMOS.E2E_TIMEOUT_ERROR_HIT_THRESHOLD_FOR_PPAF",
            "COSMOS.E2E_TIMEOUT_ERROR_HIT_TIME_WINDOW_IN_SECONDS_FOR_PPAF",
            "COSMOS.STALE_PARTITION_UNAVAILABILITY_REFRESH_INTERVAL_IN_SECONDS",
            "COSMOS.ALLOWED_PARTITION_UNAVAILABILITY_DURATION_IN_SECONDS",
            "COSMOS.PARTITION_LEVEL_CIRCUIT_BREAKER_CONFIG", // Implicitly set when COSMOS.IS_PER_PARTITION_AUTOMATIC_FAILOVER_ENABLED is set to true
            "COSMOS.THINCLIENT_ENABLED",
            "COSMOS.HTTP2_ENABLED" // Implicitly set when COSMOS.THINCLIENT_ENABLED is set to true
    );

    @Override
    public void execute(Configuration cfg) {
        Object lock = new Object();

        AtomicInteger createSuccessCount = new AtomicInteger(0);
        AtomicInteger createFailureCount = new AtomicInteger(0);
        AtomicInteger readSuccessCount = new AtomicInteger(0);
        AtomicInteger readFailureCount = new AtomicInteger(0);
        AtomicInteger querySuccessCount = new AtomicInteger(0);
        AtomicInteger queryFailureCount = new AtomicInteger(0);

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
        boolean shouldIncludeQueryWorkload = cfg.shouldExecuteQueryWorkload();

        ConnectionMode connectionMode = cfg.getConnectionMode();

        logger.info("Run Configurations : {}", cfg);

        CosmosAsyncClient cosmosAsyncClient = null;

        try {

            CosmosClientBuilder clientBuilder = new CosmosClientBuilder()
                    .endpoint(documentEndpoint)
                    .key(masterKey)
                    .preferredRegions(preferredRegions)
                    .userAgentSuffix(drillId)
                    .sessionRetryOptions(WorkloadUtils.REMOTE_REGION_PREFERRED_SESSION_RETRY_OPTIONS);

            if (connectionMode == ConnectionMode.DIRECT) {
                clientBuilder = clientBuilder.directMode();
            } else {
                clientBuilder = clientBuilder.gatewayMode();
            }

            System.setProperty("COSMOS.IS_SESSION_TOKEN_FALSE_PROGRESS_MERGE_ENABLED", "true");
            System.setProperty("COSMOS.E2E_TIMEOUT_ERROR_HIT_THRESHOLD_FOR_PPAF", "5");
            System.setProperty("COSMOS.E2E_TIMEOUT_ERROR_HIT_TIME_WINDOW_IN_SECONDS_FOR_PPAF", "120");
            System.setProperty("COSMOS.STALE_PARTITION_UNAVAILABILITY_REFRESH_INTERVAL_IN_SECONDS", "60");
            System.setProperty("COSMOS.ALLOWED_PARTITION_UNAVAILABILITY_DURATION_IN_SECONDS", "30");

            if (cfg.isThinClientEnabled()) {
                System.setProperty("COSMOS.THINCLIENT_ENABLED", "true");
                System.setProperty("COSMOS.HTTP2_ENABLED", "true");

                if (cfg.getConnectionMode() == ConnectionMode.DIRECT) {
                    throw new IllegalArgumentException("Thin Client is not supported in Direct Connection Mode");
                }

                clientBuilder = clientBuilder.gatewayMode();
            }

            boolean isSharedThroughput = cfg.isSharedThroughput();

            cosmosAsyncClient = clientBuilder.buildAsyncClient();

            if (isSharedThroughput) {
                cosmosAsyncClient.createDatabaseIfNotExists(
                                cfg.getDatabaseName(),
                                ThroughputProperties.createManualThroughput(cfg.getProvisionedThroughput()))
                        .onErrorResume(throwable -> Mono.empty())
                        .block();

                CosmosAsyncDatabase cosmosAsyncDatabase = cosmosAsyncClient.getDatabase(cfg.getDatabaseName());

                CosmosContainerProperties cosmosContainerProperties = new CosmosContainerProperties(cfg.getContainerName(), cfg.getPartitionKeyPath());
                cosmosAsyncDatabase
                        .createContainerIfNotExists(cosmosContainerProperties)
                        .onErrorResume(throwable -> Mono.empty())
                        .block();
            } else {
                cosmosAsyncClient
                        .createDatabaseIfNotExists(cfg.getDatabaseName())
                        .onErrorResume(throwable -> Mono.empty())
                        .block();

                CosmosAsyncDatabase cosmosAsyncDatabase = cosmosAsyncClient.getDatabase(cfg.getDatabaseName());

                CosmosContainerProperties cosmosContainerProperties = new CosmosContainerProperties(cfg.getContainerName(), cfg.getPartitionKeyPath());
                cosmosAsyncDatabase
                        .createContainerIfNotExists(cosmosContainerProperties, ThroughputProperties.createManualThroughput(cfg.getProvisionedThroughput()))
                        .onErrorResume(throwable -> Mono.empty())
                        .block();
            }

            CosmosAsyncDatabase cosmosAsyncDatabase = cosmosAsyncClient.getDatabase(cfg.getDatabaseName());
            CosmosAsyncContainer cosmosAsyncContainer = cosmosAsyncDatabase.getContainer(cfg.getContainerName());

            Instant startTime = Instant.now();

            // Fault Injection Setup for Reads
            // Inject Response Delay of 5s (keep injecting for 10 minutes)
            // Start injecting 3 minutes after workload has started
            // Run workload for 15 minutes
            FaultInjectionServerErrorResult faultInjectionServerErrorResult = FaultInjectionResultBuilders
                    .getResultBuilder(FaultInjectionServerErrorType.RESPONSE_DELAY)
                    .delay(Duration.ofSeconds(11))
                    .suppressServiceRequests(true)
                    .build();

            FaultInjectionCondition faultInjectionCondition = new FaultInjectionConditionBuilder()
                    .connectionType(cfg.getConnectionMode() == ConnectionMode.DIRECT ? FaultInjectionConnectionType.DIRECT : FaultInjectionConnectionType.GATEWAY)
                    .operationType(FaultInjectionOperationType.READ_ITEM)
                    .region("East US")
                    .build();

            List<FaultInjectionRule> faultInjectionRules = new ArrayList<>();

            if (cfg.shouldInjectResponseDelayForReads()) {
                for (int i = 0; i < 2; i++) {
                    FaultInjectionRule faultInjectionRule = new FaultInjectionRuleBuilder("response-delay-" + UUID.randomUUID().toString())
                            .condition(faultInjectionCondition)
                            .startDelay(Duration.ofMinutes(11 + ((i) * 30)))
                            .result(faultInjectionServerErrorResult)
                            .duration(Duration.ofMinutes(20))
                            .build();

                    faultInjectionRules.add(faultInjectionRule);
                }
            }

            CosmosFaultInjectionHelper
                    .configureFaultInjectionRules(cosmosAsyncContainer, faultInjectionRules)
                    .block();

            for (int i = 0; i < scheduledFutures.length; i++) {

                final int finalI = i;

                if (i % 3 == 0) {
                    scheduledFutures[i] = scheduledThreadPoolExecutor.schedule(() -> {
                        try {
                            WorkloadUtils.onCreate(
                                    cosmosAsyncContainer,
                                    cfg,
                                    startTime,
                                    runDuration,
                                    finalI,
                                    createSuccessCount,
                                    createFailureCount,
                                    successfullyPersistedIds,
                                    lock);
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    }, 10, TimeUnit.MILLISECONDS);
                } else if (i % 3 == 1) {
                    if (shouldIncludeReadWorkload) {
                        scheduledFutures[i] = scheduledThreadPoolExecutor.schedule(() -> {
                            try {
                                WorkloadUtils.onRead(
                                        cosmosAsyncContainer,
                                        cfg,
                                        startTime,
                                        runDuration,
                                        finalI,
                                        readSuccessCount,
                                        readFailureCount,
                                        successfullyPersistedIds,
                                        random,
                                        lock);
                            } catch (InterruptedException e) {
                                throw new RuntimeException(e);
                            }
                        }, 10, TimeUnit.MILLISECONDS);
                    }
                } else {
                    if (shouldIncludeQueryWorkload) {
                        scheduledFutures[i] = scheduledThreadPoolExecutor.schedule(() -> {
                            try {
                                WorkloadUtils.onQuery(
                                        cosmosAsyncContainer,
                                        cfg,
                                        startTime,
                                        runDuration,
                                        finalI,
                                        querySuccessCount,
                                        queryFailureCount,
                                        successfullyPersistedIds,
                                        random,
                                        lock);
                            } catch (InterruptedException e) {
                                throw new RuntimeException(e);
                            }
                        }, 10, TimeUnit.MILLISECONDS);
                    }
                }
            }

            while (!Instant.now().minus(runDuration).isAfter(startTime)) {
            }

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
