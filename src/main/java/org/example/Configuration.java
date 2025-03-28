package org.example;

import com.azure.cosmos.ConnectionMode;
import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.Parameter;

import java.time.Duration;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Locale;

public class Configuration {

    // Get current date and time in UTC
    private static final ZonedDateTime UTC_TIME = ZonedDateTime.now(ZoneId.of("UTC"));
    // Define the date format
    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("dd-MM-yyyy");

    @Parameter(names = "-accountMasterKey", description = "The master key associated with the account.", required = false)
    private String accountMasterKey = "";

    // We need to pass the DocumentEndpoint in for environments higher than Test
    // Different environments have different endpoints
    // Test environment has document endpoint
    //   <>.documents-test.windows-int.net
    // Stage environment has document endpoint
    //   <>.documents-staging.windows-ppe.net
    @Parameter(names = "-accountHost", description = "The account host URL.", required = false)
    private String accountHost = "";

    @Parameter(names = "-databaseName", description = "The database name to be used.")
    private String databaseName = "db01";

    @Parameter(names = "-containerName", description = "The container name to be used.")
    private String containerName = "ct01";

    @Parameter(names = "-runningTime", description = "The running time of the entire workload.", converter = DurationConverter.class)
    private Duration runningTime = Duration.ofMinutes(30);

    @Parameter(names = "-numberOfThreads", description = "The no. of parallel operations to run.")
    private int numberOfThreads = 2;

    @Parameter(names = "-partitionKeyPath", description = "The partition key path associated with the container.")
    private String partitionKeyPath = "/id";

    @Parameter(names = "-containerTtlInSeconds", description = "The TTL associated with a particular container.")
    private int containerTtlInSeconds = 604800;

    @Parameter(names = "-provisionedThroughput", description = "The manual provisioned throughput for the target container.")
    private int provisionedThroughput = 4000;

    @Parameter(names = "-sleepTime", description = "The duration in milliseconds between each iteration of tasks.")
    private int sleepTime = 2000;

    @Parameter(names = "-isSharedThroughput", description = "A boolean parameter to indicate whether the database is a shared throughput database.", arity = 1)
    private boolean isSharedThroughput = false;

    @Parameter(names = "-shouldLogCosmosDiagnosticsForSuccessfulResponse", description = "A boolean parameter to indicate whether the diagnostics string is logged for a successful response.", arity = 1)
    private boolean shouldLogCosmosDiagnosticsForSuccessfulResponse = false;

    @Parameter(names = "-shouldExecuteReadWorkload", description = "A boolean parameter to indicate whether point read workload should be executed.", arity = 1)
    private boolean shouldExecuteReadWorkload = true;

    @Parameter(names = "-shouldInjectResponseDelayForReads", description = "A boolean parameter to indicate whether point read workload should be injected with response delay.", arity = 1)
    private boolean shouldInjectResponseDelayForReads = false;

    @Parameter(names = "-drillId", description = "An identifier to uniquely identify a DR drill.")
    private String drillId = UTC_TIME.format(FORMATTER);;

    @Parameter(names = "-connectionMode", description = "A parameter to denote the Connection Mode to use for the client.", converter = ConnectionModeConverter.class)
    private ConnectionMode connectionMode = ConnectionMode.DIRECT;

    @Parameter(names = "-drillWorkloadType", description = "An identifier to denote whether this is a Session Consistency specific PPAF drill or Generic PPAF drill.", converter = WorkloadTypeConverter.class)
    private WorkloadType drillWorkloadType = WorkloadType.PPAFDrillWorkload;

    @Parameter(names = "-shouldUseSessionTokenOnRequestOptions", description = "A boolean parameter to indicate whether session token should be used with request options.", arity = 1)
    private boolean shouldUseSessionTokenOnRequestOptions = false;

    @Parameter(names = "-shouldHaveE2ETimeoutForWrites", description = "A boolean parameter to indicate whether writes should have e2e timeout set.", arity = 1)
    private boolean shouldHaveE2ETimeoutForWrites = false;

    public boolean shouldLogCosmosDiagnosticsForSuccessfulResponse() {
        return this.shouldLogCosmosDiagnosticsForSuccessfulResponse;
    }

    public int getSleepTime() {
        return this.sleepTime;
    }

    public Configuration setSleepTime(int sleepTime) {
        this.sleepTime = sleepTime;
        return this;
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

    public Configuration setAccountHost(String accountHost) {
        this.accountHost = accountHost;
        return this;
    }

    public String getDatabaseName() {
        return this.databaseName;
    }

    public Configuration setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
        return this;
    }

    public String getContainerName() {
        return this.containerName;
    }

    public Configuration setContainerName(String containerName) {
        this.containerName = containerName;
        return this;
    }

    public Duration getRunningTime() {
        return this.runningTime;
    }

    public Configuration setRunningTime(Duration runningTime) {
        this.runningTime = runningTime;
        return this;
    }

    public int getNumberOfThreads() {
        return this.numberOfThreads;
    }

    public Configuration setNumberOfThreads(int numberOfThreads) {
        this.numberOfThreads = numberOfThreads;
        return this;
    }

    public String getPartitionKeyPath() {
        return this.partitionKeyPath;
    }

    public Configuration setPartitionKeyPath(String partitionKeyPath) {
        this.partitionKeyPath = partitionKeyPath;
        return this;
    }

    public int getContainerTtlInSeconds() {
        return this.containerTtlInSeconds;
    }

    public Configuration setContainerTtlInSeconds(int containerTtlInSeconds) {
        this.containerTtlInSeconds = containerTtlInSeconds;
        return this;
    }

    public int getProvisionedThroughput() {
        return this.provisionedThroughput;
    }

    public Configuration setProvisionedThroughput(int provisionedThroughput) {
        this.provisionedThroughput = provisionedThroughput;
        return this;
    }

    public String getDrillId() {
        return this.drillId;
    }

    public boolean shouldExecuteReadWorkload() {
        return this.shouldExecuteReadWorkload;
    }

    public ConnectionMode getConnectionMode() {
        return this.connectionMode;
    }

    public Configuration setConnectionMode(ConnectionMode connectionMode) {
        this.connectionMode = connectionMode;
        return this;
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

    @Override
    public String toString() {
        return "Configuration{" +
                "accountHost='" + accountHost + '\'' +
                ", databaseName='" + databaseName + '\'' +
                ", containerName='" + containerName + '\'' +
                ", runningTime=" + runningTime +
                ", numberOfThreads=" + numberOfThreads +
                ", partitionKeyPath='" + partitionKeyPath + '\'' +
                ", containerTtlInSeconds=" + containerTtlInSeconds +
                ", provisionedThroughput=" + provisionedThroughput +
                ", sleepTime=" + sleepTime +
                ", isSharedThroughput=" + isSharedThroughput +
                ", drillId='" + drillId + '\'' +
                '}';
    }
}
