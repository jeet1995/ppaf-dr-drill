package org.example;

import com.azure.cosmos.CosmosAsyncClient;
import com.azure.cosmos.CosmosAsyncContainer;
import com.azure.cosmos.CosmosAsyncDatabase;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.CosmosDiagnosticsContext;
import com.azure.cosmos.CosmosException;
import com.azure.cosmos.implementation.CosmosDaemonThreadFactory;
import com.azure.cosmos.implementation.TestConfigurations;
import com.azure.cosmos.models.CosmosContainerProperties;
import com.azure.cosmos.models.ThroughputProperties;
import com.beust.jcommander.JCommander;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class Program {

    private static final Logger logger = LoggerFactory.getLogger(Program.class);


    static {
        LocalDateTime myDateObj = LocalDateTime.now();
        DateTimeFormatter myFormatObj = DateTimeFormatter.ofPattern("ddMMyyyyHHmmss");
        String formattedDate = myDateObj.format(myFormatObj);

        System.setProperty("ppafRunStart", formattedDate);
    }

    public static void main(String[] args) {
        Configuration cfg = new Configuration();

        long runId = Long.parseLong(System.getProperty("ppafRunStart"));

        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger failureCount = new AtomicInteger(0);

        logger.info("Parsing command-line args...");

        JCommander jCommander = new JCommander(cfg, null, args);

        Duration runDuration = cfg.getRunningTime();

        System.setProperty("COSMOS.IS_PER_PARTITION_AUTOMATIC_FAILOVER_ENABLED", "true");
        System.setProperty("COSMOS.IS_SESSION_TOKEN_FALSE_PROGRESS_MERGE_DISABLED", "false");

        List<String> preferredRegions = Utils.getPreferredRegions(cfg);

        int parallelism = cfg.getNumberOfThreads();

        ScheduledThreadPoolExecutor scheduledThreadPoolExecutor = new ScheduledThreadPoolExecutor(parallelism, new CosmosDaemonThreadFactory("CosmosUpsertExecutor"));
        ScheduledFuture<?>[] scheduledFutures = new ScheduledFuture[parallelism];

        String documentEndpoint = cfg.getAccountHost().isEmpty() ? TestConfigurations.HOST : cfg.getAccountHost();
        String masterKey = cfg.getAccountMasterKey().isEmpty() ? TestConfigurations.MASTER_KEY : cfg.getAccountMasterKey();

        logger.info("Run Configurations : {}", cfg);

        try (CosmosAsyncClient cosmosAsyncClient = new CosmosClientBuilder()
                .directMode()
                .endpoint(documentEndpoint)
                .key(masterKey)
                .preferredRegions(preferredRegions)
                .userAgentSuffix(String.valueOf(runId))
                .buildAsyncClient()) {

            boolean isSharedThroughput = cfg.isSharedThroughput();

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

            for (int i = 0; i < parallelism; i++) {

                final int finalI = i;

                scheduledFutures[i] = scheduledThreadPoolExecutor.schedule(() -> {
                    try {
                        onCreate(cosmosAsyncContainer, cfg, startTime, runDuration, finalI, successCount, failureCount);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }, 10, TimeUnit.MILLISECONDS);
            }

            while (!Instant.now().minus(runDuration).isAfter(startTime)) {
            }

            logger.info("Workload complete!");

            for (int i = 0; i < parallelism; i++) {
                scheduledFutures[i].cancel(true);
            }

        } finally {
            // no-op
        }
    }

    private static void onCreate(
            CosmosAsyncContainer cosmosAsyncContainer,
            Configuration cfg,
            Instant startTime,
            Duration runDuration,
            int scheduledFutureId,
            AtomicInteger successCount,
            AtomicInteger failureCount) throws InterruptedException {

        while (!Instant.now().minus(runDuration).isAfter(startTime)) {

            for (int i = 0; i < 10; i++) {
                cosmosAsyncContainer
                        .createItem(Book.build())
                        .doOnSuccess(createResponse -> {

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
                                        successCountSnapshot,
                                        failureCountSnapshot,
                                        scheduledFutureId,
                                        statusCode,
                                        subStatusCode,
                                        commaSeparatedContactedRegionNames,
                                        "",
                                        "");
                            } else {
                                requestResponseInfo = new RequestResponseInfo(
                                        timeOfResponse,
                                        successCountSnapshot,
                                        failureCountSnapshot,
                                        scheduledFutureId,
                                        statusCode,
                                        subStatusCode,
                                        commaSeparatedContactedRegionNames,
                                        createResponse.getDiagnostics().toString(),
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
            }
            Thread.sleep(cfg.getSleepTime());
        }
    }
}
