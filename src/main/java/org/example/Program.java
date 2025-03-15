package org.example;

import com.azure.cosmos.ConnectionMode;
import com.azure.cosmos.CosmosAsyncClient;
import com.azure.cosmos.CosmosAsyncContainer;
import com.azure.cosmos.CosmosAsyncDatabase;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.CosmosDiagnosticsContext;
import com.azure.cosmos.CosmosEndToEndOperationLatencyPolicyConfig;
import com.azure.cosmos.CosmosEndToEndOperationLatencyPolicyConfigBuilder;
import com.azure.cosmos.CosmosException;
import com.azure.cosmos.CosmosRegionSwitchHint;
import com.azure.cosmos.SessionRetryOptions;
import com.azure.cosmos.SessionRetryOptionsBuilder;
import com.azure.cosmos.implementation.CosmosDaemonThreadFactory;
import com.azure.cosmos.implementation.TestConfigurations;
import com.azure.cosmos.models.CosmosContainerProperties;
import com.azure.cosmos.models.CosmosItemRequestOptions;
import com.azure.cosmos.models.PartitionKey;
import com.azure.cosmos.models.ThroughputProperties;
import com.beust.jcommander.JCommander;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class Program {

    private static final Logger logger = LoggerFactory.getLogger(Program.class);

    private static final String CREATE_OP = "create";
    private static final String READ_OP = "read";

    private static final CosmosEndToEndOperationLatencyPolicyConfig E2E_POLICY_FOR_WRITE
            = new CosmosEndToEndOperationLatencyPolicyConfigBuilder(Duration.ofSeconds(7)).build();
    private static final CosmosEndToEndOperationLatencyPolicyConfig E2E_POLICY_FOR_READ
            = new CosmosEndToEndOperationLatencyPolicyConfigBuilder(Duration.ofSeconds(10)).build();

    private static final CosmosItemRequestOptions REQUEST_OPTIONS_FOR_CREATE
            = new CosmosItemRequestOptions().setCosmosEndToEndOperationLatencyPolicyConfig(E2E_POLICY_FOR_WRITE);
    private static final CosmosItemRequestOptions REQUEST_OPTIONS_FOR_READ
            = new CosmosItemRequestOptions().setCosmosEndToEndOperationLatencyPolicyConfig(E2E_POLICY_FOR_READ);

    private static final SessionRetryOptions SESSION_RETRY_OPTIONS
            = new SessionRetryOptionsBuilder()
            .maxRetriesPerRegion(2)
            .minTimeoutPerRegion(Duration.ofSeconds(10))
            .regionSwitchHint(CosmosRegionSwitchHint.REMOTE_REGION_PREFERRED)
            .build();

    private static final Integer MAX_ID_CACHE_SIZE = 100;

    public static void main(String[] args) {
        Configuration cfg = new Configuration();

        Object lock = new Object();

        AtomicInteger createSuccessCount = new AtomicInteger(0);
        AtomicInteger createFailureCount = new AtomicInteger(0);
        AtomicInteger readSuccessCount = new AtomicInteger(0);
        AtomicInteger readFailureCount = new AtomicInteger(0);

        CopyOnWriteArrayList<String> successfullyPersistedIds = new CopyOnWriteArrayList<>();
        ThreadLocalRandom random = ThreadLocalRandom.current();

        logger.info("Parsing command-line args...");

        JCommander jCommander = new JCommander(cfg, null, args);

        Duration runDuration = cfg.getRunningTime();

        System.setProperty("COSMOS.IS_PER_PARTITION_AUTOMATIC_FAILOVER_ENABLED", "true");
        System.setProperty("COSMOS.IS_SESSION_TOKEN_FALSE_PROGRESS_MERGE_ENABLED", "true");

        List<String> preferredRegions = Utils.getPreferredRegions(cfg);

        int parallelism = cfg.getNumberOfThreads();

        ScheduledThreadPoolExecutor scheduledThreadPoolExecutor = new ScheduledThreadPoolExecutor(2 * parallelism, new CosmosDaemonThreadFactory("CosmosCreateExecutor"));

        ScheduledFuture<?>[] scheduledFutures = new ScheduledFuture[2 * parallelism];

        String documentEndpoint = cfg.getAccountHost().isEmpty() ? TestConfigurations.HOST : cfg.getAccountHost();
        String masterKey = cfg.getAccountMasterKey().isEmpty() ? TestConfigurations.MASTER_KEY : cfg.getAccountMasterKey();
        String drillId = cfg.getDrillId();

        boolean shouldIncludeReadWorkload = cfg.isShouldExecuteReadWorkload();

        ConnectionMode connectionMode = cfg.getConnectionMode();

        logger.info("Run Configurations : {}", cfg);

        CosmosAsyncClient cosmosAsyncClient = null;

        try {

            CosmosClientBuilder clientBuilder = new CosmosClientBuilder()
                    .endpoint(documentEndpoint)
                    .key(masterKey)
                    .preferredRegions(preferredRegions)
                    .userAgentSuffix(drillId)
                    .sessionRetryOptions(SESSION_RETRY_OPTIONS);

            if (connectionMode == ConnectionMode.DIRECT) {
                clientBuilder = clientBuilder.directMode();
            } else {
                clientBuilder = clientBuilder.gatewayMode();
            }

            System.setProperty(
                    "COSMOS.PARTITION_LEVEL_CIRCUIT_BREAKER_CONFIG",
                    "{\"isPartitionLevelCircuitBreakerEnabled\": true, "
                            + "\"circuitBreakerType\": \"CONSECUTIVE_EXCEPTION_COUNT_BASED\","
                            + "\"consecutiveExceptionCountToleratedForReads\": 10,"
                            + "\"consecutiveExceptionCountToleratedForWrites\": 5,"
                            + "}");

            System.setProperty("COSMOS.STALE_PARTITION_UNAVAILABILITY_REFRESH_INTERVAL_IN_SECONDS", "35");
            System.setProperty("COSMOS.ALLOWED_PARTITION_UNAVAILABILITY_DURATION_IN_SECONDS", "25");

            boolean isSharedThroughput = cfg.isSharedThroughput();

            cosmosAsyncClient = clientBuilder.buildAsyncClient();

            if (isSharedThroughput) {
                cosmosAsyncClient.createDatabaseIfNotExists(
                        cfg.getDatabaseName(),
                        ThroughputProperties.createManualThroughput(cfg.getPhysicalPartitionCount() * 10_000))
                        .block();

                CosmosAsyncDatabase cosmosAsyncDatabase = cosmosAsyncClient.getDatabase(cfg.getDatabaseName());

                CosmosContainerProperties cosmosContainerProperties = new CosmosContainerProperties(cfg.getContainerName(), cfg.getPartitionKeyPath());
                cosmosAsyncDatabase.createContainerIfNotExists(cosmosContainerProperties).block();
            } else {
                cosmosAsyncClient.createDatabaseIfNotExists(cfg.getDatabaseName()).block();

                CosmosAsyncDatabase cosmosAsyncDatabase = cosmosAsyncClient.getDatabase(cfg.getDatabaseName());

                CosmosContainerProperties cosmosContainerProperties = new CosmosContainerProperties(cfg.getContainerName(), cfg.getPartitionKeyPath());
                cosmosAsyncDatabase.createContainerIfNotExists(cosmosContainerProperties, ThroughputProperties.createManualThroughput(cfg.getPhysicalPartitionCount() * 10_000)).block();
            }

            CosmosAsyncDatabase cosmosAsyncDatabase = cosmosAsyncClient.getDatabase(cfg.getDatabaseName());
            CosmosAsyncContainer cosmosAsyncContainer = cosmosAsyncDatabase.getContainer(cfg.getContainerName());

            Instant startTime = Instant.now();

            for (int i = 0; i < scheduledFutures.length; i++) {

                final int finalI = i;

                if (i % 2 == 0) {
                    scheduledFutures[i] = scheduledThreadPoolExecutor.schedule(() -> {
                        try {
                            onCreate(
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
                } else {

                    if (shouldIncludeReadWorkload) {

                        scheduledFutures[i] = scheduledThreadPoolExecutor.schedule(() -> {
                            try {
                                onRead(
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
                }
            }

            while (!Instant.now().minus(runDuration).isAfter(startTime)) {
            }

            logger.info("Workload complete!");

            for (ScheduledFuture<?> scheduledFuture : scheduledFutures) {
                scheduledFuture.cancel(true);
            }

        } finally {
            if (cosmosAsyncClient != null) {
                cosmosAsyncClient.close();
            }
        }
    }

    private static void onCreate(
            CosmosAsyncContainer cosmosAsyncContainer,
            Configuration cfg,
            Instant startTime,
            Duration runDuration,
            int scheduledFutureId,
            AtomicInteger successCount,
            AtomicInteger failureCount,
            CopyOnWriteArrayList<String> successfullyPersistedIds,
            Object lock) throws InterruptedException {

        while (!Instant.now().minus(runDuration).isAfter(startTime)) {

            for (int i = 0; i < 10; i++) {

                Book book = Book.build();

                synchronized (lock) {

                    if (successfullyPersistedIds.size() == MAX_ID_CACHE_SIZE) {
                        successfullyPersistedIds.remove(0);
                    }
                }

                cosmosAsyncContainer
                        .createItem(book, REQUEST_OPTIONS_FOR_CREATE)
                        .doOnSuccess(createResponse -> {

                            successfullyPersistedIds.add(book.getId());

                            int successCountSnapshot = successCount.incrementAndGet();
                            int failureCountSnapshot = failureCount.get();
                            int statusCode = createResponse.getStatusCode();
                            int subStatusCode = 0;

                            Instant timeOfResponse = Instant.now();

                            Set<String> contactedRegionNames = createResponse.getDiagnostics().getDiagnosticsContext().getContactedRegionNames();
                            String commaSeparatedContactedRegionNames = String.join(",", contactedRegionNames);

                            RequestResponseInfo requestResponseInfo;

                            if (cfg.shouldLogCosmosDiagnosticsForSuccessfulResponse()) {
                                requestResponseInfo = new RequestResponseInfo(
                                        timeOfResponse,
                                        CREATE_OP,
                                        cfg.getDrillId(),
                                        successCountSnapshot,
                                        failureCountSnapshot,
                                        scheduledFutureId,
                                        statusCode,
                                        subStatusCode,
                                        commaSeparatedContactedRegionNames,
                                        createResponse.getDiagnostics().toString(),
                                        "");
                            } else {
                                requestResponseInfo = new RequestResponseInfo(
                                        timeOfResponse,
                                        CREATE_OP,
                                        cfg.getDrillId(),
                                        successCountSnapshot,
                                        failureCountSnapshot,
                                        scheduledFutureId,
                                        statusCode,
                                        subStatusCode,
                                        commaSeparatedContactedRegionNames,
                                        "",
                                        "");
                            }

                            logger.info(requestResponseInfo.toString());
                        })
                        .onErrorComplete(throwable -> {

                            if (throwable instanceof CosmosException) {

                                int successCountSnapshot = successCount.get();
                                int failureCountSnapshot = failureCount.incrementAndGet();

                                CosmosException cosmosException = (CosmosException) throwable;

                                int statusCode = cosmosException.getStatusCode();
                                int subStatusCode = cosmosException.getSubStatusCode();

                                Instant timeOfResponse = Instant.now();

                                CosmosDiagnosticsContext cosmosDiagnosticsContext = cosmosException.getDiagnostics().getDiagnosticsContext();

                                Set<String> contactedRegionNames = cosmosDiagnosticsContext.getContactedRegionNames();
                                String commaSeparatedContactedRegionNames = String.join(",", contactedRegionNames);

                                RequestResponseInfo requestResponseInfo = new RequestResponseInfo(
                                        timeOfResponse,
                                        CREATE_OP,
                                        cfg.getDrillId(),
                                        successCountSnapshot,
                                        failureCountSnapshot,
                                        scheduledFutureId,
                                        statusCode,
                                        subStatusCode,
                                        commaSeparatedContactedRegionNames,
                                        cosmosException.getDiagnostics().toString(),
                                        cosmosException.getMessage());

                                logger.error(requestResponseInfo.toString());
                            }
                            return true;
                        })
                        .block();

                Thread.sleep(cfg.getSleepTime());
            }
        }
    }

    private static void onRead(
            CosmosAsyncContainer cosmosAsyncContainer,
            Configuration cfg,
            Instant startTime,
            Duration runDuration,
            int scheduledFutureId,
            AtomicInteger successCount,
            AtomicInteger failureCount,
            CopyOnWriteArrayList<String> successfullyCreatedIds,
            ThreadLocalRandom random,
            Object lock) throws InterruptedException {

        while (!Instant.now().minus(runDuration).isAfter(startTime)) {

            int chosenIndex;
            String id;

            synchronized (lock) {

                if (successfullyCreatedIds.isEmpty()) {
                    continue;
                }

                chosenIndex = random.nextInt(successfullyCreatedIds.size());
                id = successfullyCreatedIds.get(chosenIndex);
            }

            for (int i = 0; i < 10; i++) {
                cosmosAsyncContainer
                        .readItem(id, new PartitionKey(id), REQUEST_OPTIONS_FOR_READ, Book.class)
                        .doOnSuccess(readResponse -> {

                            int successCountSnapshot = successCount.incrementAndGet();
                            int failureCountSnapshot = failureCount.get();
                            int statusCode = readResponse.getStatusCode();
                            int subStatusCode = 0;

                            Instant timeOfResponse = Instant.now();

                            Set<String> contactedRegionNames = readResponse.getDiagnostics().getDiagnosticsContext().getContactedRegionNames();
                            String commaSeparatedContactedRegionNames = String.join(",", contactedRegionNames);

                            RequestResponseInfo requestResponseInfo;

                            if (cfg.shouldLogCosmosDiagnosticsForSuccessfulResponse()) {
                                requestResponseInfo = new RequestResponseInfo(
                                        timeOfResponse,
                                        READ_OP,
                                        cfg.getDrillId(),
                                        successCountSnapshot,
                                        failureCountSnapshot,
                                        scheduledFutureId,
                                        statusCode,
                                        subStatusCode,
                                        commaSeparatedContactedRegionNames,
                                        readResponse.getDiagnostics().toString(),
                                        "");
                            } else {
                                requestResponseInfo = new RequestResponseInfo(
                                        timeOfResponse,
                                        READ_OP,
                                        cfg.getDrillId(),
                                        successCountSnapshot,
                                        failureCountSnapshot,
                                        scheduledFutureId,
                                        statusCode,
                                        subStatusCode,
                                        commaSeparatedContactedRegionNames,
                                        "",
                                        "");
                            }

                            logger.info(requestResponseInfo.toString());
                        })
                        .onErrorComplete(throwable -> {

                            if (throwable instanceof CosmosException) {

                                int successCountSnapshot = successCount.get();
                                int failureCountSnapshot = failureCount.incrementAndGet();

                                CosmosException cosmosException = (CosmosException) throwable;

                                int statusCode = cosmosException.getStatusCode();
                                int subStatusCode = cosmosException.getSubStatusCode();

                                Instant timeOfResponse = Instant.now();

                                CosmosDiagnosticsContext cosmosDiagnosticsContext = cosmosException.getDiagnostics().getDiagnosticsContext();

                                Set<String> contactedRegionNames = cosmosDiagnosticsContext.getContactedRegionNames();
                                String commaSeparatedContactedRegionNames = String.join(",", contactedRegionNames);

                                RequestResponseInfo requestResponseInfo = new RequestResponseInfo(
                                        timeOfResponse,
                                        READ_OP,
                                        cfg.getDrillId(),
                                        successCountSnapshot,
                                        failureCountSnapshot,
                                        scheduledFutureId,
                                        statusCode,
                                        subStatusCode,
                                        commaSeparatedContactedRegionNames,
                                        cosmosException.getDiagnostics().toString(),
                                        cosmosException.getMessage());

                                logger.error(requestResponseInfo.toString());
                            }
                            return true;
                        })
                        .block();
                Thread.sleep(cfg.getSleepTime());
            }
        }
    }

}
