package org.example;

import com.azure.cosmos.CosmosAsyncContainer;
import com.azure.cosmos.CosmosDiagnosticsContext;
import com.azure.cosmos.CosmosEndToEndOperationLatencyPolicyConfig;
import com.azure.cosmos.CosmosEndToEndOperationLatencyPolicyConfigBuilder;
import com.azure.cosmos.CosmosException;
import com.azure.cosmos.CosmosRegionSwitchHint;
import com.azure.cosmos.SessionRetryOptions;
import com.azure.cosmos.SessionRetryOptionsBuilder;
import com.azure.cosmos.models.CosmosItemRequestOptions;
import com.azure.cosmos.models.PartitionKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class WorkloadUtils {

    private static final Logger logger = LoggerFactory.getLogger(WorkloadUtils.class);

    public static final String CREATE_OP = "create";
    public static final String READ_OP = "read";

    public static final CosmosEndToEndOperationLatencyPolicyConfig E2E_POLICY_FOR_WRITE
            = new CosmosEndToEndOperationLatencyPolicyConfigBuilder(Duration.ofSeconds(3)).build();
    public static final CosmosEndToEndOperationLatencyPolicyConfig E2E_POLICY_FOR_READ
            = new CosmosEndToEndOperationLatencyPolicyConfigBuilder(Duration.ofSeconds(6)).build();

    public static final CosmosItemRequestOptions REQUEST_OPTIONS_FOR_CREATE_WO_E2E_TIMEOUT
            = new CosmosItemRequestOptions();
    public static final CosmosItemRequestOptions REQUEST_OPTIONS_FOR_CREATE_WITH_E2E_TIMEOUT
            = new CosmosItemRequestOptions().setCosmosEndToEndOperationLatencyPolicyConfig(E2E_POLICY_FOR_WRITE);
    public static final CosmosItemRequestOptions REQUEST_OPTIONS_FOR_READ
            = new CosmosItemRequestOptions().setCosmosEndToEndOperationLatencyPolicyConfig(E2E_POLICY_FOR_READ);

    public static final SessionRetryOptions REMOTE_REGION_PREFERRED_SESSION_RETRY_OPTIONS
            = new SessionRetryOptionsBuilder()
            .maxRetriesPerRegion(2)
            .minTimeoutPerRegion(Duration.ofSeconds(10))
            .regionSwitchHint(CosmosRegionSwitchHint.REMOTE_REGION_PREFERRED)
            .build();

    public static final SessionRetryOptions LOCAL_REGION_PREFERRED_SESSION_RETRY_OPTIONS
            = new SessionRetryOptionsBuilder()
            .regionSwitchHint(CosmosRegionSwitchHint.LOCAL_REGION_PREFERRED)
            .build();

    public static final Integer MAX_ID_CACHE_SIZE = 100;

    public static void onCreate(
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
                        .createItem(book, cfg.shouldWritesHaveE2ETimeout() ? REQUEST_OPTIONS_FOR_CREATE_WITH_E2E_TIMEOUT : REQUEST_OPTIONS_FOR_CREATE_WO_E2E_TIMEOUT)
                        .doOnSuccess(createResponse -> {

                            successfullyPersistedIds.add(book.getId());

                            int successCountSnapshot = successCount.incrementAndGet();
                            int failureCountSnapshot = failureCount.get();
                            int statusCode = createResponse.getStatusCode();

                            Instant timeOfResponse = Instant.now();
                            Duration remainingTime = runDuration.minus(Duration.between(startTime, timeOfResponse));

                            Set<String> contactedRegionNames = createResponse.getDiagnostics().getDiagnosticsContext().getContactedRegionNames();
                            String commaSeparatedContactedRegionNames = String.join(",", contactedRegionNames);

                            RequestResponseInfo requestResponseInfo;

                            if (cfg.shouldLogCosmosDiagnosticsForSuccessfulResponse()) {
                                requestResponseInfo = RequestResponseInfo.builder()
                                        .timeOfResponse(timeOfResponse)
                                        .operationType(CREATE_OP)
                                        .drillId(cfg.getDrillId())
                                        .withCounts(successCountSnapshot, failureCountSnapshot)
                                        .threadId(scheduledFutureId)
                                        .withSuccessResponse(statusCode, createResponse.getDiagnostics().toString())
                                        .commaSeparatedContactedRegions(commaSeparatedContactedRegionNames)
                                        .connectionModeAsStr(cfg.getConnectionMode().name())
                                        .containerName(cfg.getContainerName())
                                        .accountName(cfg.getAccountHost())
                                        .possiblyColdStartClient(runDuration.compareTo(Duration.ofHours(1)) < 0)
                                        .databaseName(cfg.getDatabaseName())
                                        .runTimeRemaining(remainingTime)
                                        .build();
                            } else {
                                requestResponseInfo = RequestResponseInfo.builder()
                                        .timeOfResponse(timeOfResponse)
                                        .operationType(CREATE_OP)
                                        .drillId(cfg.getDrillId())
                                        .withCounts(successCountSnapshot, failureCountSnapshot)
                                        .threadId(scheduledFutureId)
                                        .withSuccessResponse(statusCode, "")
                                        .commaSeparatedContactedRegions(commaSeparatedContactedRegionNames)
                                        .connectionModeAsStr(cfg.getConnectionMode().name())
                                        .containerName(cfg.getContainerName())
                                        .accountName(cfg.getAccountHost())
                                        .possiblyColdStartClient(runDuration.compareTo(Duration.ofHours(1)) < 0)
                                        .databaseName(cfg.getDatabaseName())
                                        .runTimeRemaining(remainingTime)
                                        .build();
                            }

                            logger.info(requestResponseInfo.toString());
                        })
                        .onErrorComplete(throwable -> {

                            if (throwable instanceof CosmosException) {

                                int successCountSnapshot = successCount.get();
                                int failureCountSnapshot = failureCount.incrementAndGet();

                                CosmosException cosmosException = (CosmosException) throwable;

                                int statusCode = cosmosException.getStatusCode();

                                Instant timeOfResponse = Instant.now();
                                Duration remainingTime = runDuration.minus(Duration.between(startTime, timeOfResponse));

                                CosmosDiagnosticsContext cosmosDiagnosticsContext = cosmosException.getDiagnostics().getDiagnosticsContext();

                                Set<String> contactedRegionNames = cosmosDiagnosticsContext.getContactedRegionNames();
                                String commaSeparatedContactedRegionNames = String.join(",", contactedRegionNames);

                                RequestResponseInfo requestResponseInfo = RequestResponseInfo.builder()
                                        .timeOfResponse(timeOfResponse)
                                        .operationType(CREATE_OP)
                                        .drillId(cfg.getDrillId())
                                        .withCounts(successCountSnapshot, failureCountSnapshot)
                                        .threadId(scheduledFutureId)
                                        .withErrorResponse(statusCode, cosmosException.getSubStatusCode(), cosmosException.getMessage(), cosmosException.getDiagnostics().toString())
                                        .commaSeparatedContactedRegions(commaSeparatedContactedRegionNames)
                                        .connectionModeAsStr(cfg.getConnectionMode().name())
                                        .containerName(cfg.getContainerName())
                                        .accountName(cfg.getAccountHost())
                                        .possiblyColdStartClient(runDuration.compareTo(Duration.ofHours(1)) < 0)
                                        .databaseName(cfg.getDatabaseName())
                                        .runTimeRemaining(remainingTime)
                                        .build();

                                logger.error(requestResponseInfo.toString());
                            }
                            return true;
                        })
                        .block();

                Thread.sleep(cfg.getSleepTime());
            }
        }
    }

    public static void onCreateStopOnFirstFailure(
            CosmosAsyncContainer cosmosAsyncContainer,
            Configuration cfg,
            Instant startTime,
            Duration runDuration,
            int scheduledFutureId,
            AtomicInteger successCount,
            AtomicInteger failureCount,
            AtomicBoolean isFailureDetected,
            AtomicReference<String> latestRecordedSessionToken) throws InterruptedException {

        while (!Instant.now().minus(runDuration).isAfter(startTime) && !isFailureDetected.get()) {

            for (int i = 0; i < 1; i++) {

                Book book = Book.build();

                cosmosAsyncContainer
                        .createItem(book, REQUEST_OPTIONS_FOR_CREATE_WITH_E2E_TIMEOUT)
                        .doOnSuccess(createResponse -> {

                            int successCountSnapshot = successCount.incrementAndGet();
                            int failureCountSnapshot = failureCount.get();
                            int statusCode = createResponse.getStatusCode();

                            Instant timeOfResponse = Instant.now();
                            Duration remainingTime = runDuration.minus(Duration.between(startTime, timeOfResponse));

                            Set<String> contactedRegionNames = createResponse.getDiagnostics().getDiagnosticsContext().getContactedRegionNames();
                            String commaSeparatedContactedRegionNames = String.join(",", contactedRegionNames);

                            latestRecordedSessionToken.set(createResponse.getSessionToken());

                            RequestResponseInfo requestResponseInfo;

                            if (cfg.shouldLogCosmosDiagnosticsForSuccessfulResponse()) {
                                requestResponseInfo = RequestResponseInfo.builder()
                                        .timeOfResponse(timeOfResponse)
                                        .operationType(CREATE_OP)
                                        .drillId(cfg.getDrillId())
                                        .withCounts(successCountSnapshot, failureCountSnapshot)
                                        .threadId(scheduledFutureId)
                                        .withSuccessResponse(statusCode, createResponse.getDiagnostics().toString())
                                        .commaSeparatedContactedRegions(commaSeparatedContactedRegionNames)
                                        .connectionModeAsStr(cfg.getConnectionMode().name())
                                        .containerName(cfg.getContainerName())
                                        .accountName(cfg.getAccountHost())
                                        .possiblyColdStartClient(runDuration.compareTo(Duration.ofHours(1)) < 0)
                                        .databaseName(cfg.getDatabaseName())
                                        .runTimeRemaining(remainingTime)
                                        .build();
                            } else {
                                requestResponseInfo = RequestResponseInfo.builder()
                                        .timeOfResponse(timeOfResponse)
                                        .operationType(CREATE_OP)
                                        .drillId(cfg.getDrillId())
                                        .withCounts(successCountSnapshot, failureCountSnapshot)
                                        .threadId(scheduledFutureId)
                                        .withSuccessResponse(statusCode, "")
                                        .commaSeparatedContactedRegions(commaSeparatedContactedRegionNames)
                                        .connectionModeAsStr(cfg.getConnectionMode().name())
                                        .containerName(cfg.getContainerName())
                                        .accountName(cfg.getAccountHost())
                                        .possiblyColdStartClient(runDuration.compareTo(Duration.ofHours(1)) < 0)
                                        .databaseName(cfg.getDatabaseName())
                                        .runTimeRemaining(remainingTime)
                                        .build();
                            }

                            logger.info(requestResponseInfo.toString());
                        })
                        .onErrorComplete(throwable -> {

                            if (throwable instanceof CosmosException) {

                                int successCountSnapshot = successCount.get();
                                int failureCountSnapshot = failureCount.incrementAndGet();

                                CosmosException cosmosException = (CosmosException) throwable;

                                if (isAvailabilityRelatedFailure(cosmosException)) {
                                    isFailureDetected.compareAndSet(false, true);
                                }

                                int statusCode = cosmosException.getStatusCode();

                                Instant timeOfResponse = Instant.now();
                                Duration remainingTime = runDuration.minus(Duration.between(startTime, timeOfResponse));

                                CosmosDiagnosticsContext cosmosDiagnosticsContext = cosmosException.getDiagnostics().getDiagnosticsContext();

                                Set<String> contactedRegionNames = cosmosDiagnosticsContext.getContactedRegionNames();
                                String commaSeparatedContactedRegionNames = String.join(",", contactedRegionNames);

                                RequestResponseInfo requestResponseInfo = RequestResponseInfo.builder()
                                        .timeOfResponse(timeOfResponse)
                                        .operationType(CREATE_OP)
                                        .drillId(cfg.getDrillId())
                                        .withCounts(successCountSnapshot, failureCountSnapshot)
                                        .threadId(scheduledFutureId)
                                        .withErrorResponse(statusCode, cosmosException.getSubStatusCode(), cosmosException.getMessage(), cosmosException.getDiagnostics().toString())
                                        .commaSeparatedContactedRegions(commaSeparatedContactedRegionNames)
                                        .connectionModeAsStr(cfg.getConnectionMode().name())
                                        .containerName(cfg.getContainerName())
                                        .accountName(cfg.getAccountHost())
                                        .possiblyColdStartClient(runDuration.compareTo(Duration.ofHours(1)) < 0)
                                        .databaseName(cfg.getDatabaseName())
                                        .runTimeRemaining(remainingTime)
                                        .build();

                                logger.error(requestResponseInfo.toString());
                            }
                            return true;
                        })
                        .block();

                Thread.sleep(cfg.getSleepTime());
            }
        }
    }

    public static void onRead(
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

            for (int i = 0; i < 10; i++) {

                String idToRead;

                synchronized (lock) {
                    if (successfullyCreatedIds.isEmpty()) {
                        continue;
                    }

                    idToRead = successfullyCreatedIds.get(random.nextInt(successfullyCreatedIds.size()));
                }

                cosmosAsyncContainer
                        .readItem(idToRead, new PartitionKey(idToRead), REQUEST_OPTIONS_FOR_READ, Book.class)
                        .doOnSuccess(readResponse -> {

                            int successCountSnapshot = successCount.incrementAndGet();
                            int failureCountSnapshot = failureCount.get();
                            int statusCode = readResponse.getStatusCode();

                            Instant timeOfResponse = Instant.now();
                            Duration remainingTime = runDuration.minus(Duration.between(startTime, timeOfResponse));

                            Set<String> contactedRegionNames = readResponse.getDiagnostics().getDiagnosticsContext().getContactedRegionNames();
                            String commaSeparatedContactedRegionNames = String.join(",", contactedRegionNames);

                            RequestResponseInfo requestResponseInfo;

                            if (cfg.shouldLogCosmosDiagnosticsForSuccessfulResponse()) {
                                requestResponseInfo = RequestResponseInfo.builder()
                                        .timeOfResponse(timeOfResponse)
                                        .operationType(READ_OP)
                                        .drillId(cfg.getDrillId())
                                        .withCounts(successCountSnapshot, failureCountSnapshot)
                                        .threadId(scheduledFutureId)
                                        .withSuccessResponse(statusCode, readResponse.getDiagnostics().toString())
                                        .commaSeparatedContactedRegions(commaSeparatedContactedRegionNames)
                                        .connectionModeAsStr(cfg.getConnectionMode().name())
                                        .containerName(cfg.getContainerName())
                                        .accountName(cfg.getAccountHost())
                                        .possiblyColdStartClient(runDuration.compareTo(Duration.ofHours(1)) < 0)
                                        .databaseName(cfg.getDatabaseName())
                                        .runTimeRemaining(remainingTime)
                                        .build();
                            } else {
                                requestResponseInfo = RequestResponseInfo.builder()
                                        .timeOfResponse(timeOfResponse)
                                        .operationType(READ_OP)
                                        .drillId(cfg.getDrillId())
                                        .withCounts(successCountSnapshot, failureCountSnapshot)
                                        .threadId(scheduledFutureId)
                                        .withSuccessResponse(statusCode, "")
                                        .commaSeparatedContactedRegions(commaSeparatedContactedRegionNames)
                                        .connectionModeAsStr(cfg.getConnectionMode().name())
                                        .containerName(cfg.getContainerName())
                                        .accountName(cfg.getAccountHost())
                                        .possiblyColdStartClient(runDuration.compareTo(Duration.ofHours(1)) < 0)
                                        .databaseName(cfg.getDatabaseName())
                                        .runTimeRemaining(remainingTime)
                                        .build();
                            }

                            logger.info(requestResponseInfo.toString());
                        })
                        .onErrorComplete(throwable -> {

                            if (throwable instanceof CosmosException) {

                                int successCountSnapshot = successCount.get();
                                int failureCountSnapshot = failureCount.incrementAndGet();

                                CosmosException cosmosException = (CosmosException) throwable;

                                int statusCode = cosmosException.getStatusCode();

                                Instant timeOfResponse = Instant.now();
                                Duration remainingTime = runDuration.minus(Duration.between(startTime, timeOfResponse));

                                CosmosDiagnosticsContext cosmosDiagnosticsContext = cosmosException.getDiagnostics().getDiagnosticsContext();

                                Set<String> contactedRegionNames = cosmosDiagnosticsContext.getContactedRegionNames();
                                String commaSeparatedContactedRegionNames = String.join(",", contactedRegionNames);

                                RequestResponseInfo requestResponseInfo = RequestResponseInfo.builder()
                                        .timeOfResponse(timeOfResponse)
                                        .operationType(READ_OP)
                                        .drillId(cfg.getDrillId())
                                        .withCounts(successCountSnapshot, failureCountSnapshot)
                                        .threadId(scheduledFutureId)
                                        .withErrorResponse(statusCode, cosmosException.getSubStatusCode(), cosmosException.getMessage(), cosmosException.getDiagnostics().toString())
                                        .commaSeparatedContactedRegions(commaSeparatedContactedRegionNames)
                                        .connectionModeAsStr(cfg.getConnectionMode().name())
                                        .containerName(cfg.getContainerName())
                                        .accountName(cfg.getAccountHost())
                                        .possiblyColdStartClient(runDuration.compareTo(Duration.ofHours(1)) < 0)
                                        .databaseName(cfg.getDatabaseName())
                                        .runTimeRemaining(remainingTime)
                                        .build();

                                logger.error(requestResponseInfo.toString());
                            }
                            return true;
                        })
                        .block();

                Thread.sleep(cfg.getSleepTime());
            }
        }
    }

    public static void onSessionRead(
            CosmosAsyncContainer cosmosAsyncContainer,
            Configuration cfg,
            Instant startTime,
            Duration runDuration,
            int scheduledFutureId,
            AtomicInteger successCount,
            AtomicInteger failureCount,
            String designatedIdToRead,
            AtomicReference<String> sessionTokenFromLatestCreate) throws InterruptedException {

        while (!Instant.now().minus(runDuration).isAfter(startTime)) {

            for (int i = 0; i < 1; i++) {

                CosmosItemRequestOptions requestOptions = new CosmosItemRequestOptions()
                        .setCosmosEndToEndOperationLatencyPolicyConfig(E2E_POLICY_FOR_READ);

                if (cfg.shouldUseSessionTokenOnRequestOptions()) {
                    requestOptions.setSessionToken(sessionTokenFromLatestCreate.get());
                }

                cosmosAsyncContainer
                        .readItem(designatedIdToRead, new PartitionKey(designatedIdToRead), requestOptions, Book.class)
                        .doOnSuccess(readResponse -> {

                            int successCountSnapshot = successCount.incrementAndGet();
                            int failureCountSnapshot = failureCount.get();
                            int statusCode = readResponse.getStatusCode();

                            Instant timeOfResponse = Instant.now();
                            Duration remainingTime = runDuration.minus(Duration.between(startTime, timeOfResponse));

                            Set<String> contactedRegionNames = readResponse.getDiagnostics().getDiagnosticsContext().getContactedRegionNames();
                            String commaSeparatedContactedRegionNames = String.join(",", contactedRegionNames);

                            RequestResponseInfo requestResponseInfo;

                            if (cfg.shouldLogCosmosDiagnosticsForSuccessfulResponse()) {
                                requestResponseInfo = RequestResponseInfo.builder()
                                        .timeOfResponse(timeOfResponse)
                                        .operationType(READ_OP)
                                        .drillId(cfg.getDrillId())
                                        .withCounts(successCountSnapshot, failureCountSnapshot)
                                        .threadId(scheduledFutureId)
                                        .withSuccessResponse(statusCode, readResponse.getDiagnostics().toString())
                                        .commaSeparatedContactedRegions(commaSeparatedContactedRegionNames)
                                        .connectionModeAsStr(cfg.getConnectionMode().name())
                                        .containerName(cfg.getContainerName())
                                        .accountName(cfg.getAccountHost())
                                        .possiblyColdStartClient(runDuration.compareTo(Duration.ofHours(1)) < 0)
                                        .databaseName(cfg.getDatabaseName())
                                        .runTimeRemaining(remainingTime)
                                        .build();
                            } else {
                                requestResponseInfo = RequestResponseInfo.builder()
                                        .timeOfResponse(timeOfResponse)
                                        .operationType(READ_OP)
                                        .drillId(cfg.getDrillId())
                                        .withCounts(successCountSnapshot, failureCountSnapshot)
                                        .threadId(scheduledFutureId)
                                        .withSuccessResponse(statusCode, "")
                                        .commaSeparatedContactedRegions(commaSeparatedContactedRegionNames)
                                        .connectionModeAsStr(cfg.getConnectionMode().name())
                                        .containerName(cfg.getContainerName())
                                        .accountName(cfg.getAccountHost())
                                        .possiblyColdStartClient(runDuration.compareTo(Duration.ofHours(1)) < 0)
                                        .databaseName(cfg.getDatabaseName())
                                        .runTimeRemaining(remainingTime)
                                        .build();
                            }

                            logger.info(requestResponseInfo.toString());
                        })
                        .onErrorComplete(throwable -> {

                            if (throwable instanceof CosmosException) {

                                int successCountSnapshot = successCount.get();
                                int failureCountSnapshot = failureCount.incrementAndGet();

                                CosmosException cosmosException = (CosmosException) throwable;

                                int statusCode = cosmosException.getStatusCode();

                                Instant timeOfResponse = Instant.now();
                                Duration remainingTime = runDuration.minus(Duration.between(startTime, timeOfResponse));

                                CosmosDiagnosticsContext cosmosDiagnosticsContext = cosmosException.getDiagnostics().getDiagnosticsContext();

                                Set<String> contactedRegionNames = cosmosDiagnosticsContext.getContactedRegionNames();
                                String commaSeparatedContactedRegionNames = String.join(",", contactedRegionNames);

                                RequestResponseInfo requestResponseInfo = RequestResponseInfo.builder()
                                        .timeOfResponse(timeOfResponse)
                                        .operationType(READ_OP)
                                        .drillId(cfg.getDrillId())
                                        .withCounts(successCountSnapshot, failureCountSnapshot)
                                        .threadId(scheduledFutureId)
                                        .withErrorResponse(statusCode, cosmosException.getSubStatusCode(), cosmosException.getMessage(), cosmosException.getDiagnostics().toString())
                                        .commaSeparatedContactedRegions(commaSeparatedContactedRegionNames)
                                        .connectionModeAsStr(cfg.getConnectionMode().name())
                                        .containerName(cfg.getContainerName())
                                        .accountName(cfg.getAccountHost())
                                        .possiblyColdStartClient(runDuration.compareTo(Duration.ofHours(1)) < 0)
                                        .databaseName(cfg.getDatabaseName())
                                        .runTimeRemaining(remainingTime)
                                        .build();

                                logger.error(requestResponseInfo.toString());
                            }
                            return true;
                        })
                        .block();

                Thread.sleep(cfg.getSleepTime());
            }
        }
    }

    public static void cleanUpSystemProperties(List<String> systemPropertyKeys) {
        for (String key : systemPropertyKeys) {
            System.clearProperty(key);
        }
    }

    public static boolean isAvailabilityRelatedFailure(CosmosException cosmosException) {

        int statusCode = cosmosException.getStatusCode();

        return statusCode == 503 || statusCode == 408;
    }
}
