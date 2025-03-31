# PPAF DR Drill Workload

This project provides a workload generator for DR (Disaster Recovery) drill testing.

## Prerequisites

- Java Development Kit (JDK) 17
- Apache Maven

## Build Instructions

### Building ppaf-dr-drill-workload

1. Clone the repository:
   ```bash
   git clone https://github.com/jeet1995/ppaf-dr-drill-workload.git
   ```

2. Switch to the `master` branch:
   ```bash
   git checkout master
   ```

3. Navigate to the project directory:
   ```bash
   cd ppaf-dr-drill-workload
   ```

4. Build the module:
   ```bash
   mvn -e -Ppackage-assembly clean package
   ```

## Running the Application

### Configuration Parameters

| Parameter                  | Description                                                    | Possible Values         | Default                     |
|---------------------------|----------------------------------------------------------------|------------------------|----------------------------|
| `accountMasterKey`        | Master key associated with the account                         | String                 | *Required*                 |
| `accountHost`             | Account host URL                                               | String                 | *Required*                 |
| `databaseName`            | Name-based ID of the database                                  | String                 | `db01`                     |
| `containerName`           | Name-based ID of the container                                 | String                 | `ct01`                     |
| `runningTime`             | Duration of the workload (ISO-8601 format)                     | String                 | `PT30M`                    |
| `numberOfThreads`         | Number of parallel workers for the workload                    | Positive integer       | `2`                        |
| `shouldExecuteReadWorkload`| Enable/disable point read workload execution                   | `true`/`false`         | `true`                     |
| `drillId`                 | Unique identifier for a DR drill                               | Non-empty string       | Current date (dd-MM-yyyy)  |
| `connectionMode`          | Connection mode for the client                                 | `DIRECT`/`GATEWAY`     | `DIRECT`                   |

### Additional Configuration Parameters

The following parameters have defaults but are not commonly modified. Each parameter serves a specific purpose in configuring the workload:

#### Data Structure Configuration
- `partitionKeyPath`: The partition key path associated with the container. Default: `/id`
- `containerTtlInSeconds`: Time-To-Live (TTL) in seconds for documents in the container. Default: `604800` (7 days)
- `provisionedThroughput`: Manual provisioned throughput (RU/s) for the target container. Default: `4000`

#### Performance and Behavior Configuration
- `sleepTime`: Duration in milliseconds between each iteration of tasks. Controls the rate of operations. Default: `2000` ms
- `isSharedThroughput`: Determines if the database uses shared throughput across containers. Default: `false`
- `shouldLogCosmosDiagnosticsForSuccessfulResponse`: Enables logging of diagnostics string for successful responses. Default: `false`
- `shouldInjectResponseDelayForReads`: Controls whether point read workload should be injected with response delay. Default: `false`
- `shouldUseSessionTokenOnRequestOptions`: Determines if session token should be used with request options. Default: `false`
- `shouldHaveE2ETimeoutForWrites`: Controls whether writes should have end-to-end timeout set. Default: `false`

#### Workload Type Configuration
- `drillWorkloadType`: Specifies whether this is a Session Consistency specific PPAF drill or Generic PPAF drill. Values: `PPAFDrillWorkload` (default, for generic PPAF drill) or `PPAFForSessionConsistencyWorkload` (for session consistency specific drill)

### Execution Steps

1. After building, navigate to `ppaf-dr-drill-workload/target`
2. Copy the generated JAR to the parent directory `ppaf-dr-drill-workload`
3. Execute the application using the following command:

```bash
java -jar ppaf-dr-drill-workload-1.0-SNAPSHOT-jar-with-dependencies.jar \
    -accountMasterKey "<your-master-key>" \
    -accountHost "<your-account-host>" \
    -databaseName "db01" \
    -containerName "ct01" \
    -runningTime "PT30M" \
    -numberOfThreads 2 \
    -shouldExecuteReadWorkload "true" \
    -drillId "$(date '+%d-%m-%Y')" \
    -connectionMode "DIRECT"
```

Replace `<your-master-key>` and `<your-account-host>` with your actual credentials.