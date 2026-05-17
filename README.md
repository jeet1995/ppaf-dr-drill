# PPAF DR Drill Workload

A set of workloads to test for failover scenarios with a focus on Per-Partition Automatic Failover (PPAF).

## Overview

This application provides a comprehensive testing environment for evaluating the behavior of Cosmos DB under various failover scenarios. It specifically targets session consistency testing with configurable client settings and workload patterns.

## Features

- Configurable session token tracking for reads and writes
- Support for both Direct and Gateway connection modes
- Region-aware operations with configurable preferred regions
- Customizable throughput provisioning
- Detailed diagnostics logging with session token information
- Per-Partition Automatic Failover (PPAF) simulation and testing
- **JSON-based configuration** — use a JSON file instead of long CLI argument lists
- **Change feed workload** — monitor changes via the change feed pull model
- **Custom azure-cosmos JAR** — build and test from any branch/fork (e.g., a PR branch)

## Project Structure

```
src/
├── main/
│   ├── java/
│   │   └── org/
│   │       └── example/
│   │           ├── Book.java                              # Data model class
│   │           ├── Configuration.java                     # Configuration settings
│   │           ├── JsonConfigurationLoader.java           # JSON config file loader
│   │           ├── PPAFForSessionConsistencyWorkload.java  # Session consistency workload
│   │           ├── PPAFDrillWorkload.java                 # Main PPAF workload
│   │           ├── RequestResponseInfo.java               # Response tracking
│   │           ├── Utils.java                             # Helper utilities
│   │           ├── Workload.java                          # Workload interface
│   │           └── WorkloadUtils.java                     # Workload helper functions
│   └── resources/
│       └── log4j.properties                               # Logging configuration
build-custom-cosmos.ps1    # Script to build azure-cosmos from a dev branch (PowerShell)
build-custom-cosmos.sh     # Script to build azure-cosmos from a dev branch (bash)
sample-config.json         # Template JSON configuration file
```

## Prerequisites

- JDK 17+
- Maven 3.6+
- Azure Cosmos DB account

## Building the Project

```bash
mvn clean package -Dpackage-with-dependencies
```

## Running the Application

### Option 1: JSON configuration file (recommended)

Create a config file from the template:

```bash
cp sample-config.json my-config.json
# Edit my-config.json with your account details
```

Run with JSON config:

```bash
java -jar target/ppaf-dr-drill-workload-1.0-SNAPSHOT-jar-with-dependencies.jar \
  -configFile my-config.json
```

CLI arguments can override any JSON value:

```bash
java -jar target/ppaf-dr-drill-workload-1.0-SNAPSHOT-jar-with-dependencies.jar \
  -configFile my-config.json \
  -connectionMode GATEWAY \
  -runningTime PT1H
```

### Option 2: CLI arguments only (legacy)

```bash
java -jar target/ppaf-dr-drill-workload-1.0-SNAPSHOT-jar-with-dependencies.jar \
  -drillId "session-consistency-test" \
  -accountHost "https://your-account.documents.azure.com:443/" \
  -accountMasterKey "your-master-key" \
  -databaseName "TestDatabase" \
  -containerName "TestContainer" \
  -connectionMode "DIRECT" \
  -runningTime "PT30M" \
  -numberOfThreads 4 \
  -shouldExecuteReadWorkload true \
  -shouldExecuteQueryWorkload true \
  -shouldExecuteChangeFeedWorkload true
```

## Configuration Options

| Parameter | Description | Default |
|-----------|-------------|---------|
| configFile | Path to a JSON configuration file | (none) |
| drillId | Unique identifier for the test run | current date |
| accountHost | Cosmos DB account endpoint | (empty) |
| accountMasterKey | Cosmos DB master key | (empty) |
| databaseName | Database name | db01 |
| containerName | Container name | ct01 |
| partitionKeyPath | Partition key path | /id |
| connectionMode | Connection mode (DIRECT or GATEWAY) | DIRECT |
| drillWorkloadType | Workload type (PPAFDrillWorkload or PPAFForSessionConsistencyWorkload) | PPAFDrillWorkload |
| runningTime | Test duration in ISO-8601 duration format | PT30M |
| numberOfThreads | Number of concurrent threads | 2 |
| sleepTime | Sleep time between operations in ms | 2000 |
| provisionedThroughput | Container throughput (RU/s) | 10000 |
| containerTtlInSeconds | Container TTL | 604800 |
| isSharedThroughput | Whether the database uses shared throughput | false |
| shouldExecuteReadWorkload | Enable point read workload | true |
| shouldExecuteQueryWorkload | Enable query workload | true |
| shouldExecuteChangeFeedWorkload | Enable change feed workload | false |
| shouldLogCosmosDiagnosticsForSuccessfulResponse | Log diagnostics for successes | false |
| shouldInjectResponseDelayForReads | Inject fault injection delay for reads | false |
| shouldUseSessionTokenOnRequestOptions | Use session tokens on request options | false |
| shouldHaveE2ETimeoutForWrites | Set e2e timeout for writes | false |
| isThinClientEnabled | Enable thin client mode | false |

## Using a Custom azure-cosmos JAR

To test changes from a development branch (e.g., PR #48421):

### Step 1: Build the custom JAR

**PowerShell:**
```powershell
.\build-custom-cosmos.ps1 -Repo "jeet1995/azure-sdk-for-java" -Branch "AzCosmos_WriteAvailabilityStrategyForPPAF"
```

**Bash:**
```bash
./build-custom-cosmos.sh --repo jeet1995/azure-sdk-for-java --branch AzCosmos_WriteAvailabilityStrategyForPPAF
```

This shallow-clones the branch, builds `azure-cosmos`, and installs it to your local Maven repository.

### Step 2: Build ppaf-dr-drill-workload with the custom JAR

```bash
mvn clean package -Dpackage-with-dependencies -Plocal-cosmos -Dcosmos.version=<built-version>
```

The `<built-version>` is printed at the end of the build script output.

### Re-building after changes

Use `-SkipClone` (PS1) or `--skip-clone` (bash) to fetch latest without re-cloning:

```powershell
.\build-custom-cosmos.ps1 -Repo "jeet1995/azure-sdk-for-java" -Branch "AzCosmos_WriteAvailabilityStrategyForPPAF" -SkipClone
```

## Logging

The application uses SLF4J with Log4j for logging. The logs include detailed information about each operation, including:

- Operation type (create/read/query/changeFeed)
- Status codes
- Session tokens
- Contacted regions
- Response times

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the LICENSE file for details.