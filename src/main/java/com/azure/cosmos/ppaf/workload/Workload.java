package com.azure.cosmos.ppaf.workload;

import com.azure.cosmos.ppaf.config.Configuration;

public interface Workload {
    void execute(Configuration cfg);
}
