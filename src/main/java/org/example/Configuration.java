package org.example;

import com.azure.cosmos.ConnectionMode;
import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.Parameter;

import java.time.Duration;
import java.util.Locale;

public class Configuration {

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
    private String databaseName = "db";

    @Parameter(names = "-containerName", description = "The container name to be used.")
    private String containerName = "ct";

    @Parameter(names = "-runningTime", description = "The running time of the entire workload.", converter = DurationConverter.class)
    private Duration runningTime = Duration.ofMinutes(30);

    @Parameter(names = "-numberOfThreads", description = "The no. of parallel operations to run.")
    private int numberOfThreads = 2;

    @Parameter(names = "-partitionKeyPath", description = "The partition key path associated with the container.")
    private String partitionKeyPath = "/id";

    @Parameter(names = "-containerTtlInSeconds", description = "The TTL associated with a particular container.")
    private int containerTtlInSeconds = 604800;

    @Parameter(names = "-physicalPartitionCount", description = "The count of physical partitions required in the target container.")
    private int physicalPartitionCount = 2;

    @Parameter(names = "-sleepTime", description = "The duration in milliseconds between each iteration of tasks.")
    private int sleepTime = 1000;

    @Parameter(names = "-isSharedThroughput", description = "A boolean parameter to indicate whether the database is a shared throughput database.", arity = 1)
    private boolean isSharedThroughput = false;

    @Parameter(names = "-shouldLogCosmosDiagnosticsForSuccessfulResponse", description = "A boolean parameter to indicate whether the diagnostics string is logged for a successful response.", arity = 1)
    private boolean shouldLogCosmosDiagnosticsForSuccessfulResponse = false;

    @Parameter(names = "-shouldExecuteReadWorkload", description = "A boolean parameter to indicate whether point read workload should be executed.", arity = 1)
    private boolean shouldExecuteReadWorkload = true;

    @Parameter(names = "-drillId", description = "An identifier to uniquely identify a DR drill.")
    private String drillId = "";

    @Parameter(names = "-connectionMode", description = "A parameter to denote the Connection Mode to use for the client.", converter = ConnectionModeConverter.class)
    private ConnectionMode connectionMode = ConnectionMode.GATEWAY;

    public boolean shouldLogCosmosDiagnosticsForSuccessfulResponse() {
        return shouldLogCosmosDiagnosticsForSuccessfulResponse;
    }

    public Configuration setShouldLogCosmosDiagnosticsForSuccessfulResponse(boolean shouldLogCosmosDiagnosticsForSuccessfulResponse) {
        this.shouldLogCosmosDiagnosticsForSuccessfulResponse = shouldLogCosmosDiagnosticsForSuccessfulResponse;
        return this;
    }

    public int getSleepTime() {
        return sleepTime;
    }

    public Configuration setSleepTime(int sleepTime) {
        this.sleepTime = sleepTime;
        return this;
    }

    public boolean isSharedThroughput() {
        return isSharedThroughput;
    }

    public Configuration setSharedThroughput(boolean sharedThroughput) {
        isSharedThroughput = sharedThroughput;
        return this;
    }

    public String getAccountMasterKey() {
        return accountMasterKey;
    }

    public Configuration setAccountMasterKey(String accountMasterKey) {
        this.accountMasterKey = accountMasterKey;
        return this;
    }

    public String getAccountHost() {
        return accountHost;
    }

    public Configuration setAccountHost(String accountHost) {
        this.accountHost = accountHost;
        return this;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public Configuration setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
        return this;
    }

    public String getContainerName() {
        return containerName;
    }

    public Configuration setContainerName(String containerName) {
        this.containerName = containerName;
        return this;
    }

    public Duration getRunningTime() {
        return runningTime;
    }

    public Configuration setRunningTime(Duration runningTime) {
        this.runningTime = runningTime;
        return this;
    }

    public int getNumberOfThreads() {
        return numberOfThreads;
    }

    public Configuration setNumberOfThreads(int numberOfThreads) {
        this.numberOfThreads = numberOfThreads;
        return this;
    }

    public String getPartitionKeyPath() {
        return partitionKeyPath;
    }

    public Configuration setPartitionKeyPath(String partitionKeyPath) {
        this.partitionKeyPath = partitionKeyPath;
        return this;
    }

    public int getContainerTtlInSeconds() {
        return containerTtlInSeconds;
    }

    public Configuration setContainerTtlInSeconds(int containerTtlInSeconds) {
        this.containerTtlInSeconds = containerTtlInSeconds;
        return this;
    }

    public int getPhysicalPartitionCount() {
        return physicalPartitionCount;
    }

    public Configuration setPhysicalPartitionCount(int physicalPartitionCount) {
        this.physicalPartitionCount = physicalPartitionCount;
        return this;
    }

    public String getDrillId() {
        return drillId;
    }

    public Configuration setDrillId(String drillId) {
        this.drillId = drillId;
        return this;
    }

    public boolean isShouldLogCosmosDiagnosticsForSuccessfulResponse() {
        return shouldLogCosmosDiagnosticsForSuccessfulResponse;
    }

    public boolean isShouldExecuteReadWorkload() {
        return shouldExecuteReadWorkload;
    }

    public Configuration setShouldExecuteReadWorkload(boolean shouldExecuteReadWorkload) {
        this.shouldExecuteReadWorkload = shouldExecuteReadWorkload;
        return this;
    }

    public ConnectionMode getConnectionMode() {
        return connectionMode;
    }

    public Configuration setConnectionMode(ConnectionMode connectionMode) {
        this.connectionMode = connectionMode;
        return this;
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
                ", physicalPartitionCount=" + physicalPartitionCount +
                ", sleepTime=" + sleepTime +
                ", isSharedThroughput=" + isSharedThroughput +
                ", drillId='" + drillId + '\'' +
                '}';
    }
}
