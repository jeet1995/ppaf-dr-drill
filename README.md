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

## Project Structure

```
src/
├── main/
│   ├── java/
│   │   └── org/
│   │       └── example/
│   │           ├── Book.java                           # Data model class
│   │           ├── Configuration.java                  # Configuration settings
│   │           ├── PPAFForSessionConsistencyWorkload.java # Session consistency workload to test for false progress
│   │           ├── PPAFDrillWorkload.java # Workload to test for PPAF.
│   │           ├── RequestResponseInfo.java            # Response tracking
│   │           ├── Utils.java                          # Helper utilities
│   │           ├── Workload.java                       # Workload interface
│   │           └── WorkloadUtils.java                  # Workload helper functions
│   └── resources/
│       └── log4j.properties                            # Logging configuration
```

## Prerequisites

- JDK 11+
- Maven 3.6+
- Azure Cosmos DB account

## Building the Project

```bash
mvn clean package
```

## Running the Application

```bash
java -jar target/ppaf-dr-drill-workload-1.0-SNAPSHOT.jar \
  --drillId="session-consistency-test" \
  --accountHost="https://your-account.documents.azure.com:443/" \
  --accountMasterKey="your-master-key" \
  --databaseName="TestDatabase" \
  --containerName="TestContainer" \
  --partitionKeyPath="/id" \
  --preferredRegions="East US,West US" \
  --connectionMode="DIRECT" \
  --runningTime="PT30M" \
  --numberOfThreads=4 \
  --sleepTime=100 \
  --provisionedThroughput=400 \
  --shouldUseSessionTokenOnRequestOptions="true"
```

## Configuration Options

| Parameter | Description | Default |
|-----------|-------------|---------|
| drillId | Unique identifier for the test run | (required) |
| accountHost | Cosmos DB account endpoint | (required) |
| accountMasterKey | Cosmos DB master key | (required) |
| databaseName | Database name | (required) |
| containerName | Container name | (required) |
| partitionKeyPath | Partition key path | /id |
| preferredRegions | Comma-separated list of preferred regions | (required) |
| connectionMode | Connection mode (DIRECT or GATEWAY) | DIRECT |
| runningTime | Test duration in ISO-8601 duration format | PT1H |
| numberOfThreads | Number of concurrent threads | 4 |
| sleepTime | Sleep time between operations in ms | 100 |
| provisionedThroughput | Container throughput | 400 |
| shouldUseSessionTokenOnRequestOptions | Whether to use session tokens on request options | true |

## Logging

The application uses SLF4J with Log4j for logging. The logs include detailed information about each operation, including:

- Operation type (read/create)
- Status codes
- Session tokens
- Contacted regions
- Response times

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the LICENSE file for details.