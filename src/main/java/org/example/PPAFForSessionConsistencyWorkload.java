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
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
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
            "COSMOS.PARTITION_LEVEL_CIRCUIT_BREAKER_CONFIG"
    );

    private static final Book DESIGNATED_BOOK = Book.build("1");

    private void configureSystemProperties() {
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
    }

    private CosmosAsyncClient buildCosmosClient(Configuration cfg, List<String> preferredRegions) {
        String documentEndpoint = cfg.getAccountHost().isEmpty() ? TestConfigurations.HOST : cfg.getAccountHost();
        String masterKey = cfg.getAccountMasterKey().isEmpty() ? TestConfigurations.MASTER_KEY : cfg.getAccountMasterKey();
        String drillId = cfg.getDrillId();
        ConnectionMode connectionMode = cfg.getConnectionMode();

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

        return clientBuilder.buildAsyncClient();
    }

    private CosmosAsyncContainer setupCosmosContainer(CosmosAsyncClient cosmosAsyncClient, Configuration cfg) {
        cosmosAsyncClient
                .createDatabaseIfNotExists(cfg.getDatabaseName())
                .onErrorResume(throwable -> Mono.empty())
                .block();

        CosmosAsyncDatabase cosmosAsyncDatabase = cosmosAsyncClient.getDatabase(cfg.getDatabaseName());

        CosmosContainerProperties cosmosContainerProperties = new CosmosContainerProperties(
                cfg.getContainerName(), 
                cfg.getPartitionKeyPath()
        );

        cosmosAsyncDatabase
                .createContainerIfNotExists(
                        cosmosContainerProperties, 
                        ThroughputProperties.createManualThroughput(cfg.getProvisionedThroughput())
                )
                .onErrorResume(throwable -> Mono.empty())
                .block();

        CosmosAsyncContainer container = cosmosAsyncDatabase.getContainer(cfg.getContainerName());
        container.upsertItem(DESIGNATED_BOOK).block();
        return container;
    }

    private void scheduleWorkloads(
            ScheduledThreadPoolExecutor executor,
            ScheduledFuture<?>[] futures,
            CosmosAsyncContainer container,
            Configuration cfg,
            Instant startTime,
            AtomicInteger createSuccessCount,
            AtomicInteger createFailureCount,
            AtomicInteger readSuccessCount,
            AtomicInteger readFailureCount,
            AtomicBoolean isFailureDetectedOnCreate,
            AtomicReference<String> latestRecordedSessionTokenFromLatestCreate) {

        for (int i = 0; i < futures.length; i++) {
            final int finalI = i;
            if (i % 2 == 0) {
                futures[i] = scheduleCreateOperation(
                        executor, container, cfg, startTime, finalI,
                        createSuccessCount, createFailureCount,
                        isFailureDetectedOnCreate, latestRecordedSessionTokenFromLatestCreate
                );
            } else if (cfg.shouldExecuteReadWorkload()) {
                futures[i] = scheduleReadOperation(
                        executor, container, cfg, startTime, finalI,
                        readSuccessCount, readFailureCount,
                        latestRecordedSessionTokenFromLatestCreate
                );
            }
        }
    }

    private ScheduledFuture<?> scheduleCreateOperation(
            ScheduledThreadPoolExecutor executor,
            CosmosAsyncContainer container,
            Configuration cfg,
            Instant startTime,
            int workerId,
            AtomicInteger successCount,
            AtomicInteger failureCount,
            AtomicBoolean isFailureDetected,
            AtomicReference<String> sessionToken) {
        
        return executor.schedule(() -> {
            try {
                WorkloadUtils.onCreateStopOnFirstFailure(
                        container, cfg, startTime, cfg.getRunningTime(),
                        workerId, successCount, failureCount,
                        isFailureDetected, sessionToken);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }, 10, TimeUnit.MILLISECONDS);
    }

    private ScheduledFuture<?> scheduleReadOperation(
            ScheduledThreadPoolExecutor executor,
            CosmosAsyncContainer container,
            Configuration cfg,
            Instant startTime,
            int workerId,
            AtomicInteger successCount,
            AtomicInteger failureCount,
            AtomicReference<String> sessionToken) {
        
        return executor.schedule(() -> {
            try {
                WorkloadUtils.onSessionRead(
                        container, cfg, startTime, cfg.getRunningTime(),
                        workerId, successCount, failureCount,
                        "1", sessionToken);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }, 10, TimeUnit.MILLISECONDS);
    }

    private void waitForCompletion(
            ScheduledThreadPoolExecutor executor,
            ScheduledFuture<?>[] futures,
            Duration runDuration) {
        try {
            // Schedule shutdown task
            executor.schedule(() -> {
                logger.info("Workload duration completed, shutting down...");
                for (ScheduledFuture<?> future : futures) {
                    future.cancel(true);
                }
                executor.shutdown();
            }, runDuration.toMillis(), TimeUnit.MILLISECONDS);

            // Wait for completion
            if (!executor.awaitTermination(runDuration.toMillis() + 5000, TimeUnit.MILLISECONDS)) {
                logger.warn("Some tasks did not complete before the timeout. Force shutting down...");
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            logger.error("Workload was interrupted", e);
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void execute(Configuration cfg) {
        AtomicInteger createSuccessCount = new AtomicInteger(0);
        AtomicInteger createFailureCount = new AtomicInteger(0);
        AtomicInteger readSuccessCount = new AtomicInteger(0);
        AtomicInteger readFailureCount = new AtomicInteger(0);
        AtomicBoolean isFailureDetectedOnCreate = new AtomicBoolean(false);
        AtomicReference<String> latestRecordedSessionTokenFromLatestCreate = new AtomicReference<>("");

        int parallelism = cfg.getNumberOfThreads();
        ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(
                2 * parallelism,
                new CosmosDaemonThreadFactory("CosmosCreateExecutor")
        );
        ScheduledFuture<?>[] futures = new ScheduledFuture[2 * parallelism];

        logger.info("Run Configurations : {}", cfg);
        CosmosAsyncClient cosmosAsyncClient = null;

        try {
            configureSystemProperties();
            List<String> preferredRegions = Utils.getPreferredRegions(cfg);
            cosmosAsyncClient = buildCosmosClient(cfg, preferredRegions);
            CosmosAsyncContainer container = setupCosmosContainer(cosmosAsyncClient, cfg);

            Instant startTime = Instant.now();
            scheduleWorkloads(
                    executor, futures, container, cfg, startTime,
                    createSuccessCount, createFailureCount,
                    readSuccessCount, readFailureCount,
                    isFailureDetectedOnCreate, latestRecordedSessionTokenFromLatestCreate
            );

            waitForCompletion(executor, futures, cfg.getRunningTime());
            logger.info("Workload complete!");

        } finally {
            WorkloadUtils.cleanUpSystemProperties(CONFIGURED_SYSTEM_PROPERTIES);
            if (cosmosAsyncClient != null) {
                cosmosAsyncClient.close();
            }
        }
    }
}