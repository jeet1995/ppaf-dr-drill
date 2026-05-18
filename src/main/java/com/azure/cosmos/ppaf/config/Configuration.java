package com.azure.cosmos.ppaf.config;

import com.azure.cosmos.ConnectionMode;
import com.azure.cosmos.ReadConsistencyStrategy;
import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.Parameter;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Locale;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Configuration {

    // Get current date and time in UTC
    private static final ZonedDateTime UTC_TIME = ZonedDateTime.now(ZoneId.of("UTC"));
    // Define the date format
    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("dd-MM-yyyy");

    @Parameter(names = "-configFile", description = "Path to a JSON configuration file. CLI args override JSON values.")
    private String configFile = "";

    @Parameter(names = "-accountMasterKey", description = "The master key associated with the account.", required = false)
    @JsonProperty("accountMasterKey")
    private String accountMasterKey = "";

    // We need to pass the DocumentEndpoint in for environments higher than Test
    // Different environments have different endpoints
    // Test environment has document endpoint
    //   <>.documents-test.windows-int.net
    // Stage environment has document endpoint
    //   <>.documents-staging.windows-ppe.net
    @Parameter(names = "-accountHost", description = "The account host URL.", required = false)
    @JsonProperty("accountHost")
    private String accountHost = "";

    @Parameter(names = "-databaseName", description = "The database name to be used.")
    @JsonProperty("databaseName")
    private String databaseName = "db01";

    @Parameter(names = "-containerName", description = "The container name to be used.")
    @JsonProperty("containerName")
    private String containerName = "ct01";

    @Parameter(names = "-runningTime", description = "The running time of the entire workload.", converter = DurationConverter.class)
    @JsonProperty("runningTime")
    private Duration runningTime = Duration.ofMinutes(30);

    @Parameter(names = "-numberOfThreads", description = "The no. of parallel operations to run.")
    @JsonProperty("numberOfThreads")
    private int numberOfThreads = 2;

    @Parameter(names = "-partitionKeyPath", description = "The partition key path associated with the container.")
    @JsonProperty("partitionKeyPath")
    private String partitionKeyPath = "/id";

    @Parameter(names = "-containerTtlInSeconds", description = "The TTL associated with a particular container.")
    @JsonProperty("containerTtlInSeconds")
    private int containerTtlInSeconds = 604800;

    @Parameter(names = "-provisionedThroughput", description = "The manual provisioned throughput for the target container.")
    @JsonProperty("provisionedThroughput")
    private int provisionedThroughput = 10000;

    @Parameter(names = "-sleepTime", description = "The duration in milliseconds between each iteration of tasks.")
    @JsonProperty("sleepTime")
    private int sleepTime = 2000;

    @Parameter(names = "-isSharedThroughput", description = "A boolean parameter to indicate whether the database is a shared throughput database.", arity = 1)
    @JsonProperty("isSharedThroughput")
    private boolean isSharedThroughput = false;

    @Parameter(names = "-shouldLogCosmosDiagnosticsForSuccessfulResponse", description = "A boolean parameter to indicate whether the diagnostics string is logged for a successful response.", arity = 1)
    @JsonProperty("shouldLogCosmosDiagnosticsForSuccessfulResponse")
    private boolean shouldLogCosmosDiagnosticsForSuccessfulResponse = false;

    @Parameter(names = "-shouldExecuteReadWorkload", description = "A boolean parameter to indicate whether point read workload should be executed.", arity = 1)
    @JsonProperty("shouldExecuteReadWorkload")
    private boolean shouldExecuteReadWorkload = true;

    @Parameter(names = "-shouldExecuteQueryWorkload", description = "A boolean parameter to indicate whether query workload should be executed.", arity = 1)
    @JsonProperty("shouldExecuteQueryWorkload")
    private boolean shouldExecuteQueryWorkload = true;

    @Parameter(names = "-shouldInjectResponseDelayForReads", description = "A boolean parameter to indicate whether point read workload should be injected with response delay.", arity = 1)
    @JsonProperty("shouldInjectResponseDelayForReads")
    private boolean shouldInjectResponseDelayForReads = false;

    @Parameter(names = "-drillId", description = "An identifier to uniquely identify a DR drill.")
    @JsonProperty("drillId")
    private String drillId = UTC_TIME.format(FORMATTER);

    @Parameter(names = "-connectionMode", description = "A parameter to denote the Connection Mode to use for the client.", converter = ConnectionModeConverter.class)
    @JsonProperty("connectionMode")
    private ConnectionMode connectionMode = ConnectionMode.DIRECT;

    @Parameter(names = "-drillWorkloadType", description = "An identifier to denote whether this is a Session Consistency specific PPAF drill or Generic PPAF drill.", converter = WorkloadTypeConverter.class)
    @JsonProperty("drillWorkloadType")
    private WorkloadType drillWorkloadType = WorkloadType.PPAFDrillWorkload;

    @Parameter(names = "-shouldUseSessionTokenOnRequestOptions", description = "A boolean parameter to indicate whether session token should be used with request options.", arity = 1)
    @JsonProperty("shouldUseSessionTokenOnRequestOptions")
    private boolean shouldUseSessionTokenOnRequestOptions = false;

    @Parameter(names = "-shouldHaveE2ETimeoutForWrites", description = "A boolean parameter to indicate whether writes should have e2e timeout set.", arity = 1)
    @JsonProperty("shouldHaveE2ETimeoutForWrites")
    private boolean shouldHaveE2ETimeoutForWrites = false;

    @Parameter(names = "-shouldExecuteChangeFeedWorkload", description = "A boolean parameter to indicate whether change feed workload should be executed.", arity = 1)
    private boolean shouldExecuteChangeFeedWorkload = false;

    @Parameter(names = "-isThinClientEnabled", description = "A boolean parameter to indicate whether the thin client is enabled.", arity = 1)
    @JsonProperty("isThinClientEnabled")
    private boolean isThinClientEnabled = false;

    @Parameter(names = "-readConsistencyStrategy", description = "The read consistency strategy to use for the client. Values: DEFAULT, EVENTUAL, SESSION, LATEST_COMMITTED, GLOBAL_STRONG.", converter = ReadConsistencyStrategyConverter.class)
    @JsonProperty("readConsistencyStrategy")
    private ReadConsistencyStrategy readConsistencyStrategy = null;

    public static Configuration fromJsonFile(String filePath) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
        return mapper.readValue(new File(filePath), Configuration.class);
    }

    public String getConfigFile() {
        return this.configFile;
    }

    public boolean shouldExecuteChangeFeedWorkload() {
        return this.shouldExecuteChangeFeedWorkload;
    }

    public boolean shouldLogCosmosDiagnosticsForSuccessfulResponse() {
        return this.shouldLogCosmosDiagnosticsForSuccessfulResponse;
    }

    public int getSleepTime() {
        return this.sleepTime;
    }

    public void setSleepTime(int sleepTime) {
        this.sleepTime = sleepTime;
    }

    public boolean isSharedThroughput() {
        return this.isSharedThroughput;
    }

    public String getAccountMasterKey() {
        return this.accountMasterKey;
    }

    public String getAccountHost() {
        return this.accountHost;
    }

    public void setAccountHost(String accountHost) {
        this.accountHost = accountHost;
    }

    public String getDatabaseName() {
        return this.databaseName;
    }

    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    public String getContainerName() {
        return this.containerName;
    }

    public void setContainerName(String containerName) {
        this.containerName = containerName;
    }

    public Duration getRunningTime() {
        return this.runningTime;
    }

    public void setRunningTime(Duration runningTime) {
        this.runningTime = runningTime;
    }

    public int getNumberOfThreads() {
        return this.numberOfThreads;
    }

    public void setNumberOfThreads(int numberOfThreads) {
        this.numberOfThreads = numberOfThreads;
    }

    public String getPartitionKeyPath() {
        return this.partitionKeyPath;
    }

    public void setPartitionKeyPath(String partitionKeyPath) {
        this.partitionKeyPath = partitionKeyPath;
    }

    public int getContainerTtlInSeconds() {
        return this.containerTtlInSeconds;
    }

    public void setContainerTtlInSeconds(int containerTtlInSeconds) {
        this.containerTtlInSeconds = containerTtlInSeconds;
    }

    public int getProvisionedThroughput() {
        return this.provisionedThroughput;
    }

    public void setProvisionedThroughput(int provisionedThroughput) {
        this.provisionedThroughput = provisionedThroughput;
    }

    public String getDrillId() {
        return this.drillId;
    }

    public boolean shouldExecuteReadWorkload() {
        return this.shouldExecuteReadWorkload;
    }

    public boolean shouldExecuteQueryWorkload() {
        return this.shouldExecuteQueryWorkload;
    }

    public ConnectionMode getConnectionMode() {
        return this.connectionMode;
    }

    public void setConnectionMode(ConnectionMode connectionMode) {
        this.connectionMode = connectionMode;
    }

    public boolean shouldInjectResponseDelayForReads() {
        return this.shouldInjectResponseDelayForReads;
    }

    public WorkloadType getDrillWorkloadType() {
        return this.drillWorkloadType;
    }

    public boolean shouldUseSessionTokenOnRequestOptions() {
        return this.shouldUseSessionTokenOnRequestOptions;
    }

    public boolean shouldWritesHaveE2ETimeout() {
        return shouldHaveE2ETimeoutForWrites;
    }

    public boolean isThinClientEnabled() {
        return isThinClientEnabled;
    }

    public void setThinClientEnabled(boolean thinClientEnabled) {
        isThinClientEnabled = thinClientEnabled;
    }

    public ReadConsistencyStrategy getReadConsistencyStrategy() {
        return readConsistencyStrategy;
    }

    public void setReadConsistencyStrategy(ReadConsistencyStrategy readConsistencyStrategy) {
        this.readConsistencyStrategy = readConsistencyStrategy;
    }

    public void setAccountMasterKey(String accountMasterKey) {
        this.accountMasterKey = accountMasterKey;
    }

    public void setSharedThroughput(boolean sharedThroughput) {
        this.isSharedThroughput = sharedThroughput;
    }

    public void setShouldLogCosmosDiagnosticsForSuccessfulResponse(boolean value) {
        this.shouldLogCosmosDiagnosticsForSuccessfulResponse = value;
    }

    public void setShouldExecuteReadWorkload(boolean value) {
        this.shouldExecuteReadWorkload = value;
    }

    public void setShouldExecuteQueryWorkload(boolean value) {
        this.shouldExecuteQueryWorkload = value;
    }

    public void setShouldExecuteChangeFeedWorkload(boolean value) {
        this.shouldExecuteChangeFeedWorkload = value;
    }

    public void setShouldInjectResponseDelayForReads(boolean value) {
        this.shouldInjectResponseDelayForReads = value;
    }

    public void setDrillId(String drillId) {
        this.drillId = drillId;
    }

    public void setDrillWorkloadType(WorkloadType drillWorkloadType) {
        this.drillWorkloadType = drillWorkloadType;
    }

    public void setShouldUseSessionTokenOnRequestOptions(boolean value) {
        this.shouldUseSessionTokenOnRequestOptions = value;
    }

    public void setShouldHaveE2ETimeoutForWrites(boolean value) {
        this.shouldHaveE2ETimeoutForWrites = value;
    }


    @Override
    public String toString() {
        return String.format("""
                Configuration {
                    Database Configuration:
                    - Database Name: %s
                    - Container Name: %s
                    - Partition Key Path: %s
                    - Container TTL: %d seconds
                    - Provisioned Throughput: %d RU/s
                    - Shared Throughput: %b
                    
                    Workload Configuration:
                    - Running Time: %s
                    - Number of Threads: %d
                    - Sleep Time: %d ms
                    - Execute Read Workload: %b
                    - Execute Query Workload: %b
                    - Execute Change Feed Workload: %b
                    - Drill ID: %s
                    - Drill Workload Type: %s
                    
                    Connection Configuration:
                    - Connection Mode: %s
                    - Account Host: %s
                    - Account Master Key: %s
                    
                    Advanced Settings:
                    - Log Cosmos Diagnostics: %b
                    - Inject Response Delay for Reads: %b
                    - Use Session Token: %b
                    - E2E Timeout for Writes: %b
                    - Read Consistency Strategy: %s
                }""",
                databaseName,
                containerName,
                partitionKeyPath,
                containerTtlInSeconds,
                provisionedThroughput,
                isSharedThroughput,
                runningTime,
                numberOfThreads,
                sleepTime,
                shouldExecuteReadWorkload,
                shouldExecuteQueryWorkload,
                shouldExecuteChangeFeedWorkload,
                drillId,
                drillWorkloadType,
                connectionMode,
                accountHost,
                accountMasterKey.substring(0, Math.min(accountMasterKey.length(), 4)) + "...",
                shouldLogCosmosDiagnosticsForSuccessfulResponse,
                shouldInjectResponseDelayForReads,
                shouldUseSessionTokenOnRequestOptions,
                shouldHaveE2ETimeoutForWrites,
                readConsistencyStrategy != null ? readConsistencyStrategy.name() : "DEFAULT (account-level)"
        );
    }

    static class DurationConverter implements IStringConverter<Duration> {
        @Override
        public Duration convert(String value) {
            if (value == null) {
                return null;
            }

            return Duration.parse(value);
        }
    }

    static class ConnectionModeConverter implements IStringConverter<ConnectionMode> {

        @Override
        public ConnectionMode convert(String value) {
            String normalizedConnectionModeAsString
                    = value.toLowerCase(Locale.ROOT).replace(" ", "").trim();

            ConnectionMode result;

            if (normalizedConnectionModeAsString.equals("gateway")) {
                result = ConnectionMode.GATEWAY;
            } else {
                result = ConnectionMode.DIRECT;
            }

            return result;
        }
    }

    static class WorkloadTypeConverter implements IStringConverter<WorkloadType> {

        @Override
        public WorkloadType convert(String value) {

            if (value == null || value.isEmpty()) {
                return WorkloadType.PPAFDrillWorkload;
            }

            if (value.toLowerCase(Locale.ROOT).equals("ppafdrillworkload")) {
                return WorkloadType.PPAFDrillWorkload;
            }

            return WorkloadType.PPAFForSessionConsistencyWorkload;
        }
    }

    static class ReadConsistencyStrategyConverter implements IStringConverter<ReadConsistencyStrategy> {

        @Override
        public ReadConsistencyStrategy convert(String value) {
            if (value == null || value.isEmpty()) {
                return null;
            }

            return ReadConsistencyStrategy.valueOf(value.toUpperCase(Locale.ROOT).replace(" ", "_").trim());
        }
    }
}
