package org.example;

import com.beust.jcommander.JCommander;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WorkloadDriver {

    private static final Logger logger = LoggerFactory.getLogger(WorkloadDriver.class);

    public static void main(String[] args) {

        Configuration config = new Configuration();

        JCommander.newBuilder().addObject(config).build().parse(args);

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
