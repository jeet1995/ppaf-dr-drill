package org.example;


import com.azure.cosmos.CosmosAsyncClient;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.implementation.DatabaseAccount;
import com.azure.cosmos.implementation.DatabaseAccountLocation;
import com.azure.cosmos.implementation.GlobalEndpointManager;
import com.azure.cosmos.implementation.RxDocumentClientImpl;
import com.azure.cosmos.implementation.TestConfigurations;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class Utils {

    public static List<String> getPreferredRegions(Configuration configuration) {

        String documentEndpoint = configuration.getAccountHost().isEmpty() ? TestConfigurations.HOST : configuration.getAccountHost();
        String masterKey = configuration.getAccountMasterKey().isEmpty() ? TestConfigurations.MASTER_KEY : configuration.getAccountMasterKey();

        try (CosmosAsyncClient client = new CosmosClientBuilder()
                .endpoint(documentEndpoint)
                .key(masterKey)
                .buildAsyncClient()) {

            RxDocumentClientImpl documentClient = (RxDocumentClientImpl) ReflectionUtils.get(client, "asyncDocumentClient");
            GlobalEndpointManager globalEndpointManager = documentClient.getGlobalEndpointManager();

            DatabaseAccount databaseAccount = globalEndpointManager.getLatestDatabaseAccount();

            return new ArrayList<>(getAccountLevelLocationContext(databaseAccount, false).serviceOrderedReadableRegions);
        }
        catch (Exception e) {

            if (documentEndpoint.contains("test")) {
                return Arrays.asList("North Central US", "West US", "East Asia");
            } else {
                return Arrays.asList("West US 2", "East US 2", "North Central US");
            }
        }
    }

    private static AccountLevelLocationContext getAccountLevelLocationContext(DatabaseAccount databaseAccount, boolean writeOnly) {
        Iterator<DatabaseAccountLocation> locationIterator =
                writeOnly ? databaseAccount.getWritableLocations().iterator() : databaseAccount.getReadableLocations().iterator();

        List<String> serviceOrderedReadableRegions = new ArrayList<>();
        List<String> serviceOrderedWriteableRegions = new ArrayList<>();
        Map<String, String> regionMap = new ConcurrentHashMap<>();

        while (locationIterator.hasNext()) {
            DatabaseAccountLocation accountLocation = locationIterator.next();
            regionMap.put(accountLocation.getName(), accountLocation.getEndpoint());

            if (writeOnly) {
                serviceOrderedWriteableRegions.add(accountLocation.getName());
            } else {
                serviceOrderedReadableRegions.add(accountLocation.getName());
            }
        }

        return new AccountLevelLocationContext(
                serviceOrderedReadableRegions,
                serviceOrderedWriteableRegions,
                regionMap);
    }

    private static class AccountLevelLocationContext {
        private final List<String> serviceOrderedReadableRegions;
        private final List<String> serviceOrderedWriteableRegions;
        private final Map<String, String> regionNameToEndpoint;

        public AccountLevelLocationContext(
                List<String> serviceOrderedReadableRegions,
                List<String> serviceOrderedWriteableRegions,
                Map<String, String> regionNameToEndpoint) {

            this.serviceOrderedReadableRegions = serviceOrderedReadableRegions;
            this.serviceOrderedWriteableRegions = serviceOrderedWriteableRegions;
            this.regionNameToEndpoint = regionNameToEndpoint;
        }
    }
}
