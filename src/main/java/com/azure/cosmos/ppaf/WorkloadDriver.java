package com.azure.cosmos.ppaf;

import com.azure.cosmos.ppaf.config.Configuration;
import com.azure.cosmos.ppaf.config.JsonConfigurationLoader;
import com.azure.cosmos.ppaf.config.WorkloadType;
import com.azure.cosmos.ppaf.workload.PPAFDrillWorkload;
import com.azure.cosmos.ppaf.workload.PPAFForSessionConsistencyWorkload;
import com.azure.cosmos.ppaf.workload.Workload;
import com.beust.jcommander.JCommander;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class WorkloadDriver {

    private static final Logger logger = LoggerFactory.getLogger(WorkloadDriver.class);

    public static void main(String[] args) {

        Configuration config = new Configuration();

        // First pass: parse only to extract -configFile if present
        JCommander jCommander = JCommander.newBuilder().addObject(config).build();
        jCommander.parse(args);

        // If a config file was specified, load it (JSON values override defaults)
        if (config.getConfigFile() != null && !config.getConfigFile().isEmpty()) {
            try {
                JsonConfigurationLoader.loadFromFile(config.getConfigFile(), config);
            } catch (IOException e) {
                logger.error("Failed to load configuration file: {}", config.getConfigFile(), e);
                System.exit(1);
            }

            // Second pass: re-parse CLI args so they override JSON values.
            // Must create a fresh JCommander to avoid "option specified twice" errors.
            JCommander.newBuilder().addObject(config).build().parse(args);
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
