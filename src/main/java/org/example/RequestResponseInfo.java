package org.example;

import java.time.Duration;
import java.time.Instant;

public class RequestResponseInfo {

    private final Instant timeOfResponse;

    private final String operationType;

    private final String drillId;

    private final int successCountUntilNow;

    private final int failureCountUntilNow;

    private final int threadId;

    private final int statusCode;

    private final int subStatusCode;

    private final String commaSeparatedContactedRegions;

    private final String cosmosDiagnosticsContext;

    private final String errorMessage;

    private final String connectionModeAsStr;

    private final String containerName;

    private final String accountName;

    private final boolean possiblyColdStartClient;

    private final String databaseName;

    private final Duration runTimeRemaining;

    private final String latestRecordedSessionToken;

    private RequestResponseInfo(Builder builder) {
        this.timeOfResponse = builder.timeOfResponse;
        this.operationType = builder.operationType;
        this.drillId = builder.drillId;
        this.successCountUntilNow = builder.successCountUntilNow;
        this.failureCountUntilNow = builder.failureCountUntilNow;
        this.threadId = builder.threadId;
        this.statusCode = builder.statusCode;
        this.subStatusCode = builder.subStatusCode;
        this.commaSeparatedContactedRegions = builder.commaSeparatedContactedRegions;
        this.cosmosDiagnosticsContext = builder.cosmosDiagnosticsContext;
        this.errorMessage = builder.errorMessage;
        this.connectionModeAsStr = builder.connectionModeAsStr;
        this.containerName = builder.containerName;
        this.accountName = builder.accountName;
        this.possiblyColdStartClient = builder.possiblyColdStartClient;
        this.databaseName = builder.databaseName;
        this.runTimeRemaining = builder.runTimeRemaining;
        this.latestRecordedSessionToken = builder.latestRecordedSessionToken;
    }

    // Getters
    public Instant getTimeOfResponse() { return timeOfResponse; }
    public String getOperationType() { return operationType; }
    public String getDrillId() { return drillId; }
    public int getSuccessCountUntilNow() { return successCountUntilNow; }
    public int getFailureCountUntilNow() { return failureCountUntilNow; }
    public int getThreadId() { return threadId; }
    public int getStatusCode() { return statusCode; }
    public int getSubStatusCode() { return subStatusCode; }
    public String getCommaSeparatedContactedRegions() { return commaSeparatedContactedRegions; }
    public String getCosmosDiagnosticsContext() { return cosmosDiagnosticsContext; }
    public String getErrorMessage() { return errorMessage; }
    public String getConnectionModeAsStr() { return connectionModeAsStr; }
    public String getContainerName() { return containerName; }
    public String getAccountName() { return accountName; }
    public boolean isPossiblyColdStartClient() { return possiblyColdStartClient; }
    public String getDatabaseName() { return databaseName; }
    public Duration getRunTimeRemaining() { return runTimeRemaining; }
    public String getLatestRecordedSessionToken() { return latestRecordedSessionToken; }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private Instant timeOfResponse;
        private String operationType;
        private String drillId;
        private int successCountUntilNow;
        private int failureCountUntilNow;
        private int threadId;
        private int statusCode;
        private int subStatusCode;
        private String commaSeparatedContactedRegions;
        private String cosmosDiagnosticsContext = "";  // Default empty string
        private String errorMessage = "";  // Default empty string
        private String connectionModeAsStr;
        private String containerName;
        private String accountName;
        private boolean possiblyColdStartClient;
        private String databaseName;
        private Duration runTimeRemaining;
        private String latestRecordedSessionToken;

        private Builder() {}

        // Convenience method for success response
        public Builder withSuccessResponse(int statusCode, String cosmosDiagnosticsContext) {
            this.statusCode = statusCode;
            this.subStatusCode = 0;
            this.cosmosDiagnosticsContext = cosmosDiagnosticsContext != null ? cosmosDiagnosticsContext : "";
            this.errorMessage = "";
            return this;
        }

        // Convenience method for error response
        public Builder withErrorResponse(int statusCode, int subStatusCode, String errorMessage, String cosmosDiagnosticsContext) {
            this.statusCode = statusCode;
            this.subStatusCode = subStatusCode;
            this.errorMessage = errorMessage != null ? errorMessage : "";
            this.cosmosDiagnosticsContext = cosmosDiagnosticsContext != null ? cosmosDiagnosticsContext : "";
            return this;
        }

        // Convenience method for setting counts
        public Builder withCounts(int successCount, int failureCount) {
            this.successCountUntilNow = successCount;
            this.failureCountUntilNow = failureCount;
            return this;
        }

        public Builder timeOfResponse(Instant timeOfResponse) {
            this.timeOfResponse = timeOfResponse;
            return this;
        }

        public Builder operationType(String operationType) {
            this.operationType = operationType;
            return this;
        }

        public Builder drillId(String drillId) {
            this.drillId = drillId;
            return this;
        }

        public Builder successCountUntilNow(int successCountUntilNow) {
            this.successCountUntilNow = successCountUntilNow;
            return this;
        }

        public Builder failureCountUntilNow(int failureCountUntilNow) {
            this.failureCountUntilNow = failureCountUntilNow;
            return this;
        }

        public Builder threadId(int threadId) {
            this.threadId = threadId;
            return this;
        }

        public Builder statusCode(int statusCode) {
            this.statusCode = statusCode;
            return this;
        }

        public Builder subStatusCode(int subStatusCode) {
            this.subStatusCode = subStatusCode;
            return this;
        }

        public Builder commaSeparatedContactedRegions(String commaSeparatedContactedRegions) {
            this.commaSeparatedContactedRegions = commaSeparatedContactedRegions;
            return this;
        }

        public Builder cosmosDiagnosticsContext(String cosmosDiagnosticsContext) {
            this.cosmosDiagnosticsContext = cosmosDiagnosticsContext != null ? cosmosDiagnosticsContext : "";
            return this;
        }

        public Builder errorMessage(String errorMessage) {
            this.errorMessage = errorMessage != null ? errorMessage : "";
            return this;
        }

        public Builder connectionModeAsStr(String connectionModeAsStr) {
            this.connectionModeAsStr = connectionModeAsStr;
            return this;
        }

        public Builder containerName(String containerName) {
            this.containerName = containerName;
            return this;
        }

        public Builder accountName(String accountName) {
            this.accountName = accountName;
            return this;
        }

        public Builder possiblyColdStartClient(boolean possiblyColdStartClient) {
            this.possiblyColdStartClient = possiblyColdStartClient;
            return this;
        }

        public Builder databaseName(String databaseName) {
            this.databaseName = databaseName;
            return this;
        }

        public Builder runTimeRemaining(Duration runTimeRemaining) {
            this.runTimeRemaining = runTimeRemaining;
            return this;
        }

        public Builder latestRecordedSessionToken(String latestRecordedSessionToken) {
            this.latestRecordedSessionToken = latestRecordedSessionToken;
            return this;
        }

        public RequestResponseInfo build() {
            validateRequiredFields();
            return new RequestResponseInfo(this);
        }

        private void validateRequiredFields() {
            StringBuilder errors = new StringBuilder();

            if (timeOfResponse == null) {
                errors.append("timeOfResponse is required\n");
            }
            if (operationType == null || operationType.trim().isEmpty()) {
                errors.append("operationType is required\n");
            }
            if (drillId == null || drillId.trim().isEmpty()) {
                errors.append("drillId is required\n");
            }
            if (connectionModeAsStr == null || connectionModeAsStr.trim().isEmpty()) {
                errors.append("connectionModeAsStr is required\n");
            }
            if (containerName == null || containerName.trim().isEmpty()) {
                errors.append("containerName is required\n");
            }
            if (accountName == null || accountName.trim().isEmpty()) {
                errors.append("accountName is required\n");
            }
            if (databaseName == null || databaseName.trim().isEmpty()) {
                errors.append("databaseName is required\n");
            }
            if (runTimeRemaining == null) {
                errors.append("runTimeRemaining is required\n");
            }

            if (errors.length() > 0) {
                throw new IllegalStateException("Invalid RequestResponseInfo state:\n" + errors.toString());
            }
        }
    }

    @Override
    public String toString() {
        String timeOfResponseString = timeOfResponse.toString();

        return "RequestResponseInfo [timeOfResponse=" + timeOfResponseString + ", " +
                "operationType=" + operationType + ", " +
                "drillId=" + drillId + ", " +
                "successCountUntilNow=" + successCountUntilNow + ", " +
                "failureCountUntilNow=" + failureCountUntilNow + ", " +
                "threadId=" + threadId + ", " +
                "statusCode=" + statusCode + ", " +
                "subStatusCode=" + subStatusCode + ", " +
                "commaSeparatedContactedRegions=" + commaSeparatedContactedRegions + ", " +
                "cosmosDiagnosticsContext=" + cosmosDiagnosticsContext + ", " +
                "errorMessage=" + errorMessage + ", " +
                "connectionModeAsStr=" + connectionModeAsStr + ", " +
                "containerName=" + containerName + ", " +
                "accountName=" + accountName + ", " +
                "possiblyColdStartClient=" + possiblyColdStartClient + ", " +
                "databaseName=" + databaseName + ", " +
                "runTimeRemaining=" + runTimeRemaining + ", " +
                "latestRecordedSessionToken=" + latestRecordedSessionToken + "]";
    }
}
