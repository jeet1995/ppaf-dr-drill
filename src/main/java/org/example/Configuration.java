package org.example;

import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.Parameter;

import java.time.Duration;

public class Configuration {

    @Parameter(names = "-accountMasterKey", description = "The master key associated with the account.", required = true)
    private String accountMasterKey = "";

    // We need to pass the DocumentEndpoint in for environments higher than Test
    // Different environments have different endpoints
    // Test environment has document endpoint
    //   <>.documents-test.windows-int.net
    // Stage environment has document endpoint
    //   <>.documents-staging.windows-ppe.net
    @Parameter(names = "-accountHost", description = "The account host URL.", required = true)
    private String accountHost = "";

    @Parameter(names = "-databaseName", description = "The database name to be used.")
    private String databaseName = "db";

    @Parameter(names = "-containerName", description = "The container name to be used.")
    private String containerName = "ct";

    @Parameter(names = "-runningTime", description = "The running time of the entire workload.", converter = DurationConverter.class)
    private Duration runningTime = Duration.ofMinutes(30);

    @Parameter(names = "-numberOfThreads", description = "The no. of parallel operations to run.")
    private int numberOfThreads = 1;

    @Parameter(names = "-partitionKeyPath", description = "The partition key path associated with the container.")
    private String partitionKeyPath = "/id";

    @Parameter(names = "-containerTtlInSeconds", description = "The TTL associated with a particular container.")
    private int containerTtlInSeconds = 18_000;

    @Parameter(names = "-physicalPartitionCount", description = "The count of physical partitions required in the target container.")
    private int physicalPartitionCount = 10;

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

    static class DurationConverter implements IStringConverter<Duration> {
        @Override
        public Duration convert(String value) {
            if (value == null) {
                return null;
            }

            return Duration.parse(value);
        }
    }
}
