
1. Prerequisites

- Install JDK 17 and Maven.

2. Build `ppaf-dr-drill-workload`

- Clone the `jeet1995:ppaf-dr-drill-workload` repository and switch to the `master` branch.
- Navigate to `ppaf-dr-drill-workload`.
- Build the `ppaf-dr-drill-workload` module with the following command:

```
mvn -e -Ppackage-assembly clean package
```

3. Run the `jar` after building `ppaf-dr-drill-workload`

-Possible Configurations

| Configuration            | Configuration Description                                             | Possible values                        | Defaults                                  |
|--------------------------|-----------------------------------------------------------------------|----------------------------------------|-------------------------------------------|
| `cosmosAccountMasterKey` | The primary key used to authenticate with the Cosmos DB account       | The relevant string                    | Setting this is compulsory.               |
| `documentEndpoint`       | The account URL.                                                      | The relevant string                    | Setting this is compulsory.               |
| `databaseName`           | The name based ID of the database                                     | Some string                            | db                                        |
| `containerName`          | The name based ID of the container.                                   | Some string                            | ct                                        |
| `runningTime`            | The duration the workload is to run (represented in ISO-8601 format). | Some string                            | `PT3M`                                    |
| `numberOfThreads`        | The no. of parallel workers to use in the workload.                   | Positive integer                       | 1                                         |
| `partitionKeyPath`       | The partition key path to be used for the container.                  | `/id`                                  | `/id`                                     |
| `preferredRegions`       | String of comma-separated regions.                                    | `West US 2, South Central US, East US` | `North Central US, West US 2, Central US` |

3.2 Running the `jar`

- Navigate to the location `ppaf-dr-drill-workload/target` and run the below command with desired configurations.
- Copy-paste the JAR to the parent directory `ppaf-dr-drill-workload`
- Run the below command
```
java -jar ppaf-dr-drill-workload-1.0-SNAPSHOT-jar-with-dependencies.jar -cosmosAccountMasterKey "" -documentEndpoint "" -runningTime "PT3M"
```