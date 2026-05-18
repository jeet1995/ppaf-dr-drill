package com.azure.cosmos.ppaf;

import com.azure.cosmos.ppaf.config.Configuration;
import com.azure.cosmos.ppaf.config.WorkloadType;
import com.azure.cosmos.ppaf.workload.PPAFDrillWorkload;
import com.azure.cosmos.ppaf.workload.PPAFForSessionConsistencyWorkload;
import com.azure.cosmos.ppaf.workload.Workload;
import com.beust.jcommander.JCommander;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WorkloadDriver {

    private static final Logger logger = LoggerFactory.getLogger(WorkloadDriver.class);

    public static void main(String[] args) {

        Configuration config = new Configuration();

        // First pass: parse only to check for -configFile
        JCommander.newBuilder().addObject(config).build().parse(args);

        if (config.getConfigFile() != null && !config.getConfigFile().isEmpty()) {
            try {
                logger.info("Loading configuration from JSON file: {}", config.getConfigFile());
                config = Configuration.fromJsonFile(config.getConfigFile());
            } catch (Exception e) {
                logger.error("Failed to load configuration from JSON file: {}", config.getConfigFile(), e);
                throw new RuntimeException("Failed to load configuration from JSON file", e);
            }
        }

        logger.info("Configuration: {}", config);

        if (config.getDrillWorkloadType() == WorkloadType.PPAFDrillWorkload) {
            Workload workload = new PPAFDrillWorkload();
            logger.info("Running PPAF Drill workload");
            workload.execute(config);
        } else {
            Workload workload = new PPAFForSessionConsistencyWorkload();
            logger.info("Running PPAF For Session Consistency Drill workload");
            workload.execute(config);
        }
    }
}
