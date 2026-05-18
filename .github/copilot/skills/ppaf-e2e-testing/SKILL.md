---
name: ppaf-e2e-testing
description: >-
  Full end-to-end PPAF testing workflow: Service Fabric connectivity,
  test account provisioning, SDK build, workload execution, and monitoring.
  Use this skill when the user says "test ppaf", "ppaf e2e", "create ppaf
  account", "connect to SF", "run ppaf drill", "ppaf test environment",
  "set up ppaf testing", or "verify ppaf". USE FOR: connecting to Service
  Fabric test clusters, creating PPAF-enabled Cosmos DB test accounts,
  building custom azure-cosmos JARs, running ppaf-dr-drill workloads, and
  monitoring drill results. DO NOT USE FOR: CTL redeployment, stopping SF
  nodes, inducing quorum loss, or production environment operations.
---

# PPAF E2E Testing — Full Lifecycle Skill

You orchestrate the complete PPAF testing lifecycle:
**SF connectivity → account provisioning → SDK build → workload config → execution → monitoring**.

---

## ⛔ Safety Rules

- **NEVER** run CTL redeployment or `UpdateFederation` commands
- **NEVER** stop SF nodes (`StopClusterNodes`, `StartClusterNodes`)
- **NEVER** hardcode or commit credentials, account keys, tenant IDs, or certificate thumbprints
- **ALWAYS** use `ask_user` for sensitive inputs (account endpoints, keys)
- **ALWAYS** confirm with user before injecting quorum loss

---

## Paths & Prerequisites

```
RDTools:         C:\src\RDTools
Developer repo:  C:\src\Developer
ppaf-dr-drill:   C:\Users\abhmohanty\GitHub\ppaf-dr-drill
```

Requirements:
- Windows PowerShell 5.1 (SF modules don't work in PS Core)
- JDK 17+, Maven 3.9+
- VPN connected (MSFT-AzVPN-Manual)
- Valid SF admin certificate in `CurrentUser` store

---

## Phase 1 — Service Fabric Connectivity

### 1.1 Certificate Check

Verify a valid (non-expired) `sf-admin.test.infra.cdb-dev.azclient.ms` cert exists:

```powershell
Get-ChildItem Cert:\CurrentUser\My |
  Where-Object { $_.Subject -like "*sf-admin*cdb-dev*" -and $_.NotAfter -gt (Get-Date) } |
  Select-Object Thumbprint, Subject, NotAfter
```

If no valid cert, download from Key Vault (requires Azure CLI logged into the Microsoft corp tenant):

```powershell
az login --tenant <microsoft-corp-tenant-id>
$certPath = "$env:TEMP\test-devinfra-sfadmin.pfx"
az keyvault secret download --vault-name "cosmosdb-ctl" --name "test-devinfra-sfadmin" --file $certPath --encoding base64
Import-PfxCertificate -FilePath $certPath -CertStoreLocation Cert:\CurrentUser\My -Exportable
Remove-Item $certPath -Force
```

**IMPORTANT:** The tenant ID for Key Vault access must come from user context or `ask_user` — never hardcode it.

### 1.2 Connect to SF Cluster

**Must use Windows PowerShell (5.1), not PS Core.**

```powershell
powershell.exe -NoProfile -Command {
    cd C:\src\RDTools\DocumentDBPSModule
    Import-Module .\DocumentDBPSModule.psd1 -WarningAction SilentlyContinue
    Import-Module ..\AzurePowershell\Azure.psd1 -WarningAction SilentlyContinue
    Set-KeyVaultUsage -EnableUserAuth
    Enable-DevInfraCertificateLookup

    .\ConnectWinFabricCluster.ps1 -CertificateStore CurrentUser -Environment <env> -Endpoint <endpoint>
    Get-ServiceFabricClusterHealth | Select-Object AggregatedHealthState
}
```

**Ask the user** for environment and endpoint:

```
Question: "Which test environment?"
Choices:
  1. "Test14 (North Central US, West US, East Asia)"
  2. "Test61 (North Central US, West US 2, Central US)"
```

Environment → endpoint mapping:

| Environment | Write Region Endpoint                    | Other Endpoints                                      |
|-------------|------------------------------------------|------------------------------------------------------|
| Test14      | cdb-ms-test14-northcentralus1-be1        | cdb-ms-test14-westus1-be1, cdb-ms-test14-eastasia1-be1 |
| Test61      | cdb-ms-test61-northcentralus1-be1        | cdb-ms-test61-westus2-be1, cdb-ms-test61-centralus1-be1 |

### 1.3 Verify Environment Health (Optional)

```powershell
# Inside the same Windows PowerShell session:
Get-ServiceFabricApplication -ApplicationTypeName SingleServiceMasterServerApplication |
  Select-Object ApplicationTypeVersion, HealthState
```

**Note:** Cluster health "Error" is common in test environments and does not block account creation.

---

## Phase 2 — Account Provisioning

### 2.1 Drill Account Matrix

**Ask the user** what kind of drill they're running:

```
Question: "What type of drill are you setting up?"
Choices:
  1. "Full DR drill (PPAF + non-PPAF accounts, Strong + Session)"
  2. "Quick validation (single PPAF account)"
  3. "Custom — I'll specify the accounts"
```

**Full DR drill** creates a standard matrix for comparison:

| Account Pattern | Consistency | Regions | PPAF | Purpose |
|-----------------|------------|---------|------|---------|
| `<prefix>-strong-3r` | Strong | 3 | Yes | Primary PPAF validation |
| `<prefix>-session-3r` | Session | 3 | Yes | Session consistency comparison |
| `<prefix>-non-ppaf` | Strong | 3 | No | Control (baseline, expected outage) |

**Ask the user** for the account prefix:

```
Question: "What account name prefix? (e.g., abhm-test14-0319)"
(Allow freeform — suggest: "<username>-<env>-<MMdd>")
```

Region mappings per environment:

| Environment | 3-region locations | 2-region locations |
|-------------|--------------------|--------------------|
| Test14 | North Central US, West US, East Asia | North Central US, West US |
| Test61 | North Central US, West US 2, Central US | North Central US, West US 2 |

### 2.2 Create Accounts

Run inside the same Windows PowerShell session that connected to SF. Use a **shared SubscriptionId** across all accounts in the drill.

```powershell
$SubscriptionId = New-Guid
$RDToolsDir = "C:\src\RDTools"
$Environment = "<env>"  # Test14 or Test61
$Prefix = "<prefix>"
$Locations = "<region1>", "<region2>", "<region3>"

# PPAF Strong account
C:\src\Developer\ppaf\PPAFTesting\CreatePPAFAccount.ps1 `
    -AccountName "$Prefix-strong-3r" `
    -SubscriptionId $SubscriptionId `
    -Environment $Environment `
    -ShouldCreateCustomerAccount `
    -ShouldEnablePPAFOnCustomerAccount `
    -CustomerAccountLocations $Locations `
    -RDToolsDir $RDToolsDir

# PPAF Session account
C:\src\Developer\ppaf\PPAFTesting\CreatePPAFAccount.ps1 `
    -AccountName "$Prefix-session-3r" `
    -SubscriptionId $SubscriptionId `
    -Environment $Environment `
    -ShouldCreateCustomerAccount `
    -ShouldEnablePPAFOnCustomerAccount `
    -CustomerAccountLocations $Locations `
    -RDToolsDir $RDToolsDir `
    -Consistency "Session"

# Non-PPAF control account (omit -ShouldEnablePPAFOnCustomerAccount)
C:\src\Developer\ppaf\PPAFTesting\CreatePPAFAccount.ps1 `
    -AccountName "$Prefix-non-ppaf" `
    -SubscriptionId $SubscriptionId `
    -Environment $Environment `
    -ShouldCreateCustomerAccount `
    -CustomerAccountLocations $Locations `
    -RDToolsDir $RDToolsDir
```

Each account takes ~2 min per region (3 regions × 3 accounts ≈ 18 min). **Do not cancel** — the script runs silently until done.

### 2.3 Retrieve Account Credentials

After creation, retrieve endpoints and keys for all accounts:

```powershell
foreach ($acct in @("$Prefix-strong-3r", "$Prefix-session-3r", "$Prefix-non-ppaf")) {
    $ds = & $RDToolsDir\DocumentDBPSModule\GetDocumentService.ps1 `
        -Environment $Environment -SubscriptionId $SubscriptionId -Name $acct
    Write-Host "$acct  Endpoint=$($ds.DocumentEndpoint)  KeyLen=$($ds.PrimaryMasterKey.Length)"
}
```

**Store endpoints and keys in memory only** — never write to files or commit.

---

## Phase 3 — SDK Build (Custom azure-cosmos JAR)

This phase uses the `ppaf-dr-drill` skill. **Ask the user** for SDK source:

```
Question: "Where should I get the azure-cosmos JAR from?"
Choices:
  1. "Build from a branch (e.g., PR branch or fork)"
  2. "Use the latest public release from Maven Central"
  3. "Use what's already in local .m2 (skip build)"
```

### Option 1: Build from branch

**Ask the user** which repo and branch to build from:

```
Question: "Which repository and branch should I build from?"
Choices:
  1. "jeet1995/azure-sdk-for-java @ AzCosmos_WriteAvailabilityStrategyForPPAF (PR #48421)"
  2. "Azure/azure-sdk-for-java @ main"
  3. "Let me specify a custom repo/branch"
```

If custom, ask for the repo (`owner/repo` format) and branch name separately.

Then build:

```powershell
cd C:\Users\abhmohanty\GitHub\ppaf-dr-drill
.\build-custom-cosmos.ps1 -Repo "<owner/repo>" -Branch "<branch>"

# If already cloned before, use -SkipClone for speed
.\build-custom-cosmos.ps1 -Repo "<owner/repo>" -Branch "<branch>" -SkipClone
```

Capture the `Built version: X.Y.Z` from output.

### Option 2: Latest public release

```powershell
$response = Invoke-RestMethod "https://search.maven.org/solrsearch/select?q=g:com.azure+AND+a:azure-cosmos&rows=1&wt=json"
$latestVersion = $response.response.docs[0].latestVersion
```

### Option 3: Check local .m2

```powershell
Get-ChildItem "$env:USERPROFILE\.m2\repository\com\azure\azure-cosmos" -Directory |
  Sort-Object LastWriteTime -Descending |
  Select-Object Name, LastWriteTime -First 5
```

---

## Phase 4 — Build & Run Workload

### 4.1 Build ppaf-dr-drill

```powershell
cd C:\Users\abhmohanty\GitHub\ppaf-dr-drill

# With custom JAR:
mvn clean package -Dpackage-with-dependencies -Plocal-cosmos -Dcosmos.version=<version>

# With public release:
mvn clean package -Dpackage-with-dependencies
```

### 4.2 Configure the Workload

**Ask the user** for workload configuration:

```
Question: "Which workloads should run?"
Choices:
  1. "All (create + read + query + change feed)"
  2. "Create + read + query (no change feed)"
  3. "Create only"
```

```
Question: "How long should the drill run?"
Choices: ["5 minutes (PT5M)", "30 minutes (PT30M)", "1 hour (PT1H)", "2 hours (PT2H)"]
```

```
Question: "Which connection modes?"
Choices:
  1. "Both DIRECT and GATEWAY (Recommended for drill comparison)"
  2. "DIRECT only"
  3. "GATEWAY only"
```

Create **separate JSON configs per connection mode** (e.g., `drill-direct.config.json` and `drill-gateway.config.json`):

```json
{
  "databaseName": "db01",
  "containerName": "ct01",
  "partitionKeyPath": "/id",
  "connectionMode": "DIRECT",
  "drillWorkloadType": "PPAFDrillWorkload",
  "runningTime": "PT5M",
  "numberOfThreads": 2,
  "sleepTime": 2000,
  "provisionedThroughput": 10000,
  "shouldExecuteReadWorkload": true,
  "shouldExecuteQueryWorkload": true,
  "shouldExecuteChangeFeedWorkload": true
}
```

**Credentials must be CLI args, never in the JSON file.**

### 4.3 Run the Workload

For a **single account + single connection mode**:

```powershell
cd C:\Users\abhmohanty\GitHub\ppaf-dr-drill

java -jar target/ppaf-dr-drill-workload-1.0-SNAPSHOT-jar-with-dependencies.jar `
  -configFile drill-direct.config.json `
  -accountHost "<endpoint>" `
  -accountMasterKey "<key>"
```

### 4.4 Run Against All Accounts (Full Matrix)

For a full drill, run **each account × each connection mode** as separate detached processes:

```powershell
cd C:\Users\abhmohanty\GitHub\ppaf-dr-drill
$timestamp = Get-Date -Format "yyyyMMdd-HHmmss"

# Define the account matrix — endpoints and keys from Phase 2
$accounts = @(
    @{ Name="<prefix>-strong-3r"; Host="<endpoint>"; Key="<key>" },
    @{ Name="<prefix>-session-3r"; Host="<endpoint>"; Key="<key>" },
    @{ Name="<prefix>-non-ppaf"; Host="<endpoint>"; Key="<key>" }
)

$modes = @("DIRECT", "GATEWAY")

foreach ($acct in $accounts) {
    foreach ($mode in $modes) {
        $logFile = "ppaf-drill-$($acct.Name)-$mode-$timestamp.log"
        $errFile = "ppaf-drill-$($acct.Name)-$mode-$timestamp-err.log"

        Start-Process -NoNewWindow -FilePath "java" -ArgumentList @(
            "-jar", "target/ppaf-dr-drill-workload-1.0-SNAPSHOT-jar-with-dependencies.jar",
            "-configFile", "drill-direct.config.json",
            "-connectionMode", $mode,
            "-accountHost", $acct.Host,
            "-accountMasterKey", $acct.Key
        ) -RedirectStandardOutput $logFile -RedirectStandardError $errFile

        Write-Host "Started: $($acct.Name) [$mode] -> $logFile"
    }
}
```

This launches 6 processes (3 accounts × 2 modes). Monitor all with:

```powershell
Get-Content ppaf-drill-*.log -Tail 5 -Wait
```

---

## Phase 4.5 — Quorum Loss Drill (Failover + Failback)

Quorum loss is a Service Fabric API that simulates partition-level outage **without stopping nodes**. This is the recommended way to test PPAF failover behavior.

### 4.5.1 Get Partition IDs

Use Kusto to discover the partitions for the target account:

```python
from azure.kusto.data import KustoClient, KustoConnectionStringBuilder
from azure.identity import AzureCliCredential

cred = AzureCliCredential()
kcsb = KustoConnectionStringBuilder.with_azure_token_credential(
    'https://cosmosdbtest.kusto.windows.net', cred)
client = KustoClient(kcsb)
result = client.execute('Test', """
ReportQuota5M
| where TIMESTAMP > ago(6h)
| where GlobalDatabaseAccountName startswith "<account>"
| where Tenant == "<write-region-endpoint>"
| summarize arg_max(TIMESTAMP, *) by Tenant, PartitionId, ServiceName
| project PartitionId, ServiceName, DatabaseName, CollectionName
""")
for r in list(result.primary_results[0]):
    print(f"PartitionId={r[0]}  ServiceName={r[1]}  DB={r[2]}  Coll={r[3]}")
```

### 4.5.2 Drill Procedure

**Ask the user** for drill parameters:

```
Question: "What type of quorum loss drill?"
Choices:
  1. "Short validation (5 min QL, 15 min total)"
  2. "Full failover+failback (10 min QL, 30 min total)"
  3. "Custom duration"
```

**Full drill timeline** (recommended):

| Phase | Duration | What happens |
|-------|----------|-------------|
| Warmup | 5 min | Warm clients establish connections, baseline traffic |
| Quorum loss | 10 min | Writes should fail over to secondary region |
| Cold start | During QL | Cold clients start 1 min after QL (test new client during outage) |
| Recovery | ~5 min | QL ends, writes should fail back to primary region |
| Failback observation | 10 min | Verify all writes returned to primary |

**Step 1 — Launch warm clients** (DIRECT + GATEWAY, run for full drill duration):

```powershell
cd C:\Users\abhmohanty\GitHub\ppaf-dr-drill
$acctHost = "<endpoint>"
$acctKey = "<key>"

# Warm DIRECT
Start-Process -NoNewWindow -FilePath "java" -ArgumentList @(
    "-jar", "target\ppaf-dr-drill-workload-1.0-SNAPSHOT-jar-with-dependencies.jar",
    "-configFile", "<config>", "-accountHost", $acctHost, "-accountMasterKey", $acctKey
) -RedirectStandardOutput "fb-warm-DIRECT.log" -RedirectStandardError "fb-warm-DIRECT-err.log"

# Warm GATEWAY
Start-Process -NoNewWindow -FilePath "java" -ArgumentList @(
    "-jar", "target\ppaf-dr-drill-workload-1.0-SNAPSHOT-jar-with-dependencies.jar",
    "-configFile", "<config>", "-connectionMode", "GATEWAY",
    "-accountHost", $acctHost, "-accountMasterKey", $acctKey
) -RedirectStandardOutput "fb-warm-GATEWAY.log" -RedirectStandardError "fb-warm-GATEWAY-err.log"
```

**Step 2 — Wait for warmup**, then inject quorum loss (Windows PowerShell):

```powershell
Start-Sleep -Seconds 300  # 5 min warmup

powershell.exe -NoProfile -Command {
    cd C:\src\RDTools\DocumentDBPSModule
    Import-Module .\DocumentDBPSModule.psd1 -WarningAction SilentlyContinue
    Import-Module ..\AzurePowershell\Azure.psd1 -WarningAction SilentlyContinue
    Set-KeyVaultUsage -EnableUserAuth
    Enable-DevInfraCertificateLookup
    .\ConnectWinFabricCluster.ps1 -CertificateStore CurrentUser -Environment <env> -Endpoint <write-region-endpoint>

    # Inject QL on each db01 partition (skip __db__ system partition)
    $Op = New-Guid
    Start-ServiceFabricPartitionQuorumLoss -OperationId $Op `
        -QuorumLossMode AllReplicas `
        -QuorumLossDurationInSeconds <duration> `
        -PartitionId "<partition-id>" `
        -ServiceName "<service-name>"
    Write-Host "QL Start=$((Get-Date).ToUniversalTime().ToString('yyyy-MM-dd HH:mm:ss')) OpId=$Op"
}
```

**Step 3 — Launch cold clients** 1 min after QL injection (same config, shorter duration).

**Step 4 — Wait for all workloads to complete**, then analyze.

### 4.5.3 What to Verify in Kusto

After the drill, query `BackendEndRequest5M` to verify:

1. **Failover**: During QL window, successful Creates should appear in the secondary region (e.g., West US) instead of the primary (e.g., North Central US).

2. **Failback**: After QL ends, successful Creates should return to the primary region within 1–2 Kusto 5M buckets (~5–10 min).

3. **403:3 errors**: Expected during failover/failback transitions — regions being granted/revoked write privileges.

4. **No prolonged outage**: There should be successful Creates in every 5M bucket (no gap).

---

## Phase 5 — Monitor & Analyze

### Live Monitoring

```powershell
# Tail all logs
Get-Content ppaf-drill-*.log -Tail 20 -Wait

# Check running Java processes
Get-Process java -ErrorAction SilentlyContinue | Select-Object Id, CPU, WorkingSet64
```

### Client-Side Log Analysis

```powershell
# Operation counts per log file
foreach ($f in (Get-ChildItem ppaf-drill-*.log)) {
    $raw = Get-Content $f.FullName -Raw
    $creates = [regex]::Matches($raw, 'ionType=create[,\s]').Count
    $reads = [regex]::Matches($raw, 'ionType=read[,\s]').Count
    $queries = [regex]::Matches($raw, 'ionType=query[,\s]').Count
    $cf = [regex]::Matches($raw, 'ionType=changeFeed[,\s]').Count
    $errors = [regex]::Matches($raw, '(?m)^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} ERROR').Count
    Write-Host "$($f.Name): create=$creates read=$reads query=$queries cf=$cf errors=$errors"
}
```

### Kusto Analysis (BackendEndRequest5M)

Kusto cluster: `cosmosdbtest.kusto.windows.net` / Database: `Test`

Access via Python (`azure-kusto-data` + `azure-identity` must be installed):

```python
from azure.kusto.data import KustoClient, KustoConnectionStringBuilder
from azure.identity import AzureCliCredential

cred = AzureCliCredential()
kcsb = KustoConnectionStringBuilder.with_azure_token_credential(
    'https://cosmosdbtest.kusto.windows.net', cred)
client = KustoClient(kcsb)
result = client.execute('Test', '<KQL_QUERY>')
```

#### Success timechart (writes + reads by region)

```kql
let prefix = "<account-prefix>";
let t0 = datetime(<start>);
let t1 = datetime(<end>);
let partition_lst = ReportQuota5M
| where TIMESTAMP between(t0 .. t1)
| where GlobalDatabaseAccountName startswith prefix
| distinct PartitionId;
BackendEndRequest5M
| where TIMESTAMP between(t0 .. t1)
| where PartitionId in (partition_lst)
| where Res(ResourceType) == "Document"
| where Op(OperationType) in ("Read", "Create")
| extend Mode = iff(UserAgent has "java", "Direct", "Gateway")
| where StatusCode < 400
| summarize sum(SampleCount) by strcat_delim("-",
    Op(OperationType), Region, Mode, GlobalDatabaseAccountName), bin(TIMESTAMP, 5m)
| render timechart with (ymin = 0)
```

#### Error analysis (4xx/5xx breakdown)

```kql
let prefix = "<account-prefix>";
let t0 = datetime(<start>);
let t1 = datetime(<end>);
let partition_lst = ReportQuota5M
| where TIMESTAMP between(t0 .. t1)
| where GlobalDatabaseAccountName startswith prefix
| distinct PartitionId;
BackendEndRequest5M
| where TIMESTAMP between(t0 .. t1)
| where PartitionId in (partition_lst)
| where Res(ResourceType) == "Document"
| where Op(OperationType) in ("Read", "Create")
| extend Mode = iff(UserAgent has "java", "Direct", "Gateway")
| where StatusCode >= 400
| summarize sum(SampleCount) by strcat_delim("-",
    Op(OperationType), Region, Mode, StatusCode, SubStatusCode), bin(TIMESTAMP, 5m)
| render timechart with (ymin = 0)
```

#### Failback verification (post-outage, writes should return to write region)

```kql
let prefix = "<account-prefix>";
let t0 = datetime(<recovery-time>);
let t1 = t0 + 1h;
let partition_lst = ReportQuota5M
| where TIMESTAMP between(t0 .. t1)
| where GlobalDatabaseAccountName startswith prefix
| distinct PartitionId;
BackendEndRequest5M
| where TIMESTAMP between(t0 .. t1)
| where PartitionId in (partition_lst)
| where Res(ResourceType) == "Document"
| where Op(OperationType) == 0 // Create
| where StatusCode < 400
| summarize sum(SampleCount) by Region, bin(TIMESTAMP, 5m)
| render timechart with (ymin = 0)
```

### What to Look For

| Signal | Healthy | Unhealthy |
|--------|---------|-----------|
| Writes during outage | Shift to secondary within ~30s | Prolonged gap or 5xx errors |
| Reads during outage | Continue from secondary regions | 410:1022 or complete drop |
| Failback after recovery | All writes return to primary within 15 min | Writes stuck on secondary |
| Gateway vs Direct | Both fail over (Gateway may be slower) | Gateway client crashes or stalls |
| Non-PPAF control | Complete write outage during outage | N/A (expected to fail) |
| 403:3 during outage | Expected from non-write regions | Unexpected if persistent after recovery |

---

## Common Pitfalls

1. **PS Core vs Windows PowerShell** — SF modules only work in Windows PowerShell 5.1 (`powershell.exe`, not `pwsh`)
2. **Expired SF certs** — download fresh from Key Vault if `Get-ChildItem Cert:\CurrentUser\My` shows all expired
3. **LocalMachine cert store needs admin** — always use `CurrentUser` store unless running elevated
4. **Cluster health "Error"** — normal in test envs, doesn't block operations
5. **Account creation shows no progress** — the script runs silently; don't cancel it
6. **Credentials in JSON are ignored** — `accountHost`/`accountMasterKey` must be CLI args
7. **Fat JAR must be rebuilt** after changing the azure-cosmos version
8. **Thin client + DIRECT mode** — throws `IllegalArgumentException`; use GATEWAY with thin client
