package org.example;

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

    public RequestResponseInfo(
            Instant timeOfResponse,
            String operationType,
            String drillId,
            int successCountUntilNow,
            int failureCountUntilNow,
            int threadId,
            int statusCode,
            int subStatusCode,
            String commaSeparatedContactedRegions,
            String cosmosDiagnosticsContext,
            String errorMessage) {

        this.timeOfResponse = timeOfResponse;
        this.operationType = operationType;
        this.drillId = drillId;
        this.successCountUntilNow = successCountUntilNow;
        this.failureCountUntilNow = failureCountUntilNow;
        this.threadId = threadId;
        this.statusCode = statusCode;
        this.subStatusCode = subStatusCode;
        this.commaSeparatedContactedRegions = commaSeparatedContactedRegions;
        this.cosmosDiagnosticsContext = cosmosDiagnosticsContext;
        this.errorMessage = errorMessage;
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
                "errorMessage=" + errorMessage + "]";
    }
}
