package com.azure.cosmos.ppaf.config;

import com.azure.cosmos.ConnectionMode;
import com.azure.cosmos.ReadConsistencyStrategy;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.Locale;

/**
 * Loads configuration from a JSON file. Fields present in the JSON file
 * are applied to the Configuration object. Any CLI arguments parsed after
 * this will override JSON values, providing a layered config approach:
 * defaults -> JSON file -> CLI args.
 */
public class JsonConfigurationLoader {

    private static final Logger logger = LoggerFactory.getLogger(JsonConfigurationLoader.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    /**
     * Loads configuration values from a JSON file into the given Configuration object.
     * Only fields present in the JSON are overwritten; others keep their defaults.
     */
    public static void loadFromFile(String filePath, Configuration config) throws IOException {
        File file = new File(filePath);
        if (!file.exists()) {
            throw new IOException("Configuration file not found: " + filePath);
        }

        logger.info("Loading configuration from JSON file: {}", filePath);
        JsonNode root = OBJECT_MAPPER.readTree(file);

        // accountHost and accountMasterKey are intentionally excluded from JSON config
        // to prevent credentials from being stored in files. They must be passed as CLI arguments.
        if (root.has("accountHost") || root.has("accountMasterKey")) {
            logger.warn("accountHost and accountMasterKey in JSON config are ignored — pass them as CLI arguments.");
        }
        if (root.has("databaseName")) {
            config.setDatabaseName(root.get("databaseName").asText());
        }
        if (root.has("containerName")) {
            config.setContainerName(root.get("containerName").asText());
        }
        if (root.has("runningTime")) {
            config.setRunningTime(Duration.parse(root.get("runningTime").asText()));
        }
        if (root.has("numberOfThreads")) {
            config.setNumberOfThreads(root.get("numberOfThreads").asInt());
        }
        if (root.has("partitionKeyPath")) {
            config.setPartitionKeyPath(root.get("partitionKeyPath").asText());
        }
        if (root.has("containerTtlInSeconds")) {
            config.setContainerTtlInSeconds(root.get("containerTtlInSeconds").asInt());
        }
        if (root.has("provisionedThroughput")) {
            config.setProvisionedThroughput(root.get("provisionedThroughput").asInt());
        }
        if (root.has("sleepTime")) {
            config.setSleepTime(root.get("sleepTime").asInt());
        }
        if (root.has("isSharedThroughput")) {
            config.setSharedThroughput(root.get("isSharedThroughput").asBoolean());
        }
        if (root.has("shouldLogCosmosDiagnosticsForSuccessfulResponse")) {
            config.setShouldLogCosmosDiagnosticsForSuccessfulResponse(root.get("shouldLogCosmosDiagnosticsForSuccessfulResponse").asBoolean());
        }
        if (root.has("shouldExecuteReadWorkload")) {
            config.setShouldExecuteReadWorkload(root.get("shouldExecuteReadWorkload").asBoolean());
        }
        if (root.has("shouldExecuteQueryWorkload")) {
            config.setShouldExecuteQueryWorkload(root.get("shouldExecuteQueryWorkload").asBoolean());
        }
        if (root.has("shouldExecuteChangeFeedWorkload")) {
            config.setShouldExecuteChangeFeedWorkload(root.get("shouldExecuteChangeFeedWorkload").asBoolean());
        }
        if (root.has("shouldInjectResponseDelayForReads")) {
            config.setShouldInjectResponseDelayForReads(root.get("shouldInjectResponseDelayForReads").asBoolean());
        }
        if (root.has("drillId")) {
            config.setDrillId(root.get("drillId").asText());
        }
        if (root.has("connectionMode")) {
            String mode = root.get("connectionMode").asText().toLowerCase(Locale.ROOT).trim();
            config.setConnectionMode("gateway".equals(mode) ? ConnectionMode.GATEWAY : ConnectionMode.DIRECT);
        }
        if (root.has("drillWorkloadType")) {
            String type = root.get("drillWorkloadType").asText().toLowerCase(Locale.ROOT).trim();
            if ("ppafdrillworkload".equals(type)) {
                config.setDrillWorkloadType(WorkloadType.PPAFDrillWorkload);
            } else {
                config.setDrillWorkloadType(WorkloadType.PPAFForSessionConsistencyWorkload);
            }
        }
        if (root.has("shouldUseSessionTokenOnRequestOptions")) {
            config.setShouldUseSessionTokenOnRequestOptions(root.get("shouldUseSessionTokenOnRequestOptions").asBoolean());
        }
        if (root.has("shouldHaveE2ETimeoutForWrites")) {
            config.setShouldHaveE2ETimeoutForWrites(root.get("shouldHaveE2ETimeoutForWrites").asBoolean());
        }
        if (root.has("isThinClientEnabled")) {
            config.setThinClientEnabled(root.get("isThinClientEnabled").asBoolean());
        }
        if (root.has("readConsistencyStrategy") && !root.get("readConsistencyStrategy").isNull()) {
            String strategy = root.get("readConsistencyStrategy").asText().toUpperCase(Locale.ROOT).replace(" ", "_").trim();
            config.setReadConsistencyStrategy(ReadConsistencyStrategy.valueOf(strategy));
        }

        logger.info("Configuration loaded from JSON file successfully");
    }
}
