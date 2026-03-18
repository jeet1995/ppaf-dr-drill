---
name: ppaf-dr-drill
description: >-
  End-to-end PPAF DR drill workflow: discover SDK source (dev branch or latest
  public artifact), build azure-cosmos, rebuild the drill workload, configure,
  and run the workload. Use this skill when the user says "run ppaf drill",
  "ppaf workload", "dr drill", "run ppaf-dr-drill", "test ppaf", or
  "run failover workload". USE FOR: running PPAF DR drill workloads,
  building custom azure-cosmos JARs for drill testing, configuring and
  executing ppaf-dr-drill-workload. DO NOT USE FOR: running SDK unit/integration
  tests (use docker-network-fault-testing), setting up Cosmos DB accounts
  (use cosmos-thin-client-setup), RBAC setup (use cosmos-rbac-setup).
---

# PPAF DR Drill — End-to-End Workload Skill

You orchestrate the full lifecycle of running a PPAF DR drill workload:
**SDK source selection → build → workload configuration → execution**.

---

## When to Use

- User asks to "run ppaf drill", "run the workload", "ppaf-dr-drill", "dr drill"
- User asks to test PPAF failover behavior with a custom SDK build
- User asks to run the ppaf-dr-drill-workload against a Cosmos DB account
- User wants to validate a PR branch (e.g., write availability strategy) via the drill

---

## Repository Location

The ppaf-dr-drill-workload repo is at:
```
C:\Users\abhmohanty\Documents\GitHub\General\ppaf-dr-drill-workload
```

---

## Step 1 — Choose SDK Source (MUST use ask_user)

**Always prompt the user** with `ask_user` to choose the azure-cosmos source:

```
Question: "Where should I get the azure-cosmos JAR from?"

Choices:
  1. "Build from a branch (e.g., PR branch or fork)"
  2. "Use the latest public release from Maven Central"
  3. "Use what's already in local .m2 (skip build)"
```

### Option 1: Build from a branch

Ask the user for repo and branch using `ask_user`:

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
cd C:\Users\abhmohanty\Documents\GitHub\General\ppaf-dr-drill-workload

# Use the build script
.\build-custom-cosmos.ps1 -Repo "<owner/repo>" -Branch "<branch>"

# If already cloned before, use -SkipClone for speed
.\build-custom-cosmos.ps1 -Repo "<owner/repo>" -Branch "<branch>" -SkipClone
```

Capture the built version from the script output. It will print:
```
Built version: X.Y.Z-beta.N
```

### Option 2: Latest public release

Discover the latest version from the Azure SDK Maven feed:

```powershell
# Query Maven Central for latest azure-cosmos version
$response = Invoke-RestMethod "https://search.maven.org/solrsearch/select?q=g:com.azure+AND+a:azure-cosmos&rows=1&wt=json"
$latestVersion = $response.response.docs[0].latestVersion
Write-Host "Latest public azure-cosmos version: $latestVersion"
```

No build needed — Maven will download it automatically.

### Option 3: Use existing local .m2

Check what's already installed:

```powershell
# Find locally installed azure-cosmos versions
Get-ChildItem "$env:USERPROFILE\.m2\repository\com\azure\azure-cosmos" -Directory |
  Sort-Object LastWriteTime -Descending |
  Select-Object Name, LastWriteTime -First 5
```

Ask user to confirm which version to use.

---

## Step 2 — Build ppaf-dr-drill-workload

Build with the appropriate profile:

```powershell
cd C:\Users\abhmohanty\Documents\GitHub\General\ppaf-dr-drill-workload

# If using a custom-built JAR (Option 1 or specific Option 3 version):
mvn clean package -Dpackage-with-dependencies -Plocal-cosmos -Dcosmos.version=<version>

# If using the latest public release (Option 2):
# Update pom.xml azure-cosmos version, then:
mvn clean package -Dpackage-with-dependencies
```

Verify the fat JAR was created:

```powershell
$jar = Get-Item "target\ppaf-dr-drill-workload-1.0-SNAPSHOT-jar-with-dependencies.jar"
Write-Host "JAR size: $([math]::Round($jar.Length/1MB, 2)) MB, built at: $($jar.LastWriteTime)"
```

---

## Step 3 — Configure the Workload (MUST use ask_user)

### Credentials

**Always prompt for credentials** — they cannot come from JSON config:

```
Question: "Which Cosmos DB account should I run the drill against?"
(Allow freeform — user provides account host URL)
```

```
Question: "What is the account master key?"
(Allow freeform)
```

Alternatively, check if cosmos-v4.properties or a local properties file exists:

```powershell
# Check for existing credentials
$propsFiles = @(
    "cosmos-v4.properties",
    "$env:USERPROFILE\Documents\GitHub\General\azure-sdk-for-java\sdk\cosmos\cosmos-v4.properties",
    "$env:USERPROFILE\Documents\GitHub\General\azure-sdk-for-java-2\sdk\cosmos\cosmos-v4.properties"
)
foreach ($f in $propsFiles) {
    if (Test-Path $f) {
        Write-Host "Found credentials file: $f"
        $host_line = Select-String "ACCOUNT_HOST" $f | Where-Object { $_.Line -notmatch "^#" } | Select-Object -First 1
        Write-Host "  $($host_line.Line)"
    }
}
```

If a credentials file is found, ask the user if they want to use it.

### Workload Configuration

**Prompt the user** for the workload configuration approach:

```
Question: "How would you like to configure the workload?"

Choices:
  1. "Use an existing JSON config file"
  2. "Create a new JSON config (I'll ask you the key settings)"
  3. "Use CLI arguments only"
```

If creating a new config, prompt for key settings:

```
Question: "Which connection mode?"
Choices: ["DIRECT (Recommended)", "GATEWAY"]
```

```
Question: "Which workloads should run?"
Choices:
  1. "All (create + read + query + change feed)"
  2. "Create + read + query (no change feed)"
  3. "Create only"
  4. "Let me pick individually"
```

```
Question: "How long should the drill run?"
Choices: ["30 minutes (PT30M)", "1 hour (PT1H)", "2 hours (PT2H)", "Custom duration"]
```

```
Question: "Number of parallel threads?"
Choices: ["2 (light)", "4 (moderate) (Recommended)", "8 (heavy)", "Custom"]
```

Generate the JSON config file from the answers (save as `drill-config.json` in the repo root).

---

## Step 4 — Run the Workload

```powershell
cd C:\Users\abhmohanty\Documents\GitHub\General\ppaf-dr-drill-workload

java -jar target/ppaf-dr-drill-workload-1.0-SNAPSHOT-jar-with-dependencies.jar `
  -configFile drill-config.json `
  -accountHost "<account-host>" `
  -accountMasterKey "<master-key>"
```

**IMPORTANT:**
- Run this as an async detached process so it survives session shutdown
- Pipe output to a log file for later analysis
- The workload runs for the configured duration and stops automatically

```powershell
$timestamp = Get-Date -Format "yyyyMMdd-HHmmss"
$logFile = "ppaf-drill-$timestamp.log"

Start-Process -NoNewWindow -FilePath "java" -ArgumentList @(
    "-jar", "target/ppaf-dr-drill-workload-1.0-SNAPSHOT-jar-with-dependencies.jar",
    "-configFile", "drill-config.json",
    "-accountHost", "<account-host>",
    "-accountMasterKey", "<master-key>"
) -RedirectStandardOutput $logFile -RedirectStandardError "ppaf-drill-$timestamp-err.log"

Write-Host "Drill started. Logging to: $logFile"
Write-Host "Monitor with: Get-Content $logFile -Tail 20 -Wait"
```

---

## Step 5 — Monitor and Analyze

### Live monitoring

```powershell
# Tail the log
Get-Content ppaf-drill-*.log -Tail 20 -Wait

# Count successes/failures by operation type
Select-String "operationType" ppaf-drill-*.log | Group-Object { ($_ -split '"operationType":"')[1].Split('"')[0] }

# Check contacted regions
Select-String "contactedRegions" ppaf-drill-*.log | Select-Object -Last 10
```

### Post-drill analysis

```powershell
# Summary of status codes
Select-String "statusCode" ppaf-drill-*.log | Group-Object { ($_ -split '"statusCode":')[1].Split(',')[0] }

# Any failures?
Select-String "ERROR" ppaf-drill-*.log | Select-Object -Last 20

# Region distribution
Select-String "contactedRegions" ppaf-drill-*.log |
  ForEach-Object { ($_ -split '"contactedRegions":"')[1].Split('"')[0] } |
  Group-Object | Sort-Object Count -Descending
```

---

## Common Pitfalls

1. **Credentials in JSON config are ignored** — `accountHost` and `accountMasterKey` MUST be CLI args. If present in JSON, a warning is logged.
2. **Fat JAR not rebuilt after SDK change** — always `mvn clean package -Dpackage-with-dependencies` after building a new azure-cosmos JAR.
3. **Wrong cosmos.version** — the version printed by `build-custom-cosmos.ps1` must match the `-Dcosmos.version=` used in the ppaf-dr-drill build.
4. **TestConfigurations fallback** — if `accountHost`/`accountMasterKey` are empty, the code falls back to `TestConfigurations.HOST`/`MASTER_KEY` from azure-cosmos-test. This only works if the test JAR has valid properties.
5. **OOM with many threads** — each thread creates Reactor schedulers. Use `-Xmx4g` or more for `numberOfThreads > 4`.
6. **Thin client + DIRECT mode** — throws `IllegalArgumentException`. Thin client only works with GATEWAY mode.

---

## Quick Reference

| Action | Command |
|--------|---------|
| Build custom SDK | `.\build-custom-cosmos.ps1 -Repo "jeet1995/azure-sdk-for-java" -Branch "AzCosmos_WriteAvailabilityStrategyForPPAF"` |
| Rebuild drill (custom JAR) | `mvn clean package -Dpackage-with-dependencies -Plocal-cosmos -Dcosmos.version=X.Y.Z` |
| Rebuild drill (public JAR) | `mvn clean package -Dpackage-with-dependencies` |
| Run with JSON config | `java -jar target/ppaf-dr-drill-workload-1.0-SNAPSHOT-jar-with-dependencies.jar -configFile config.json -accountHost ... -accountMasterKey ...` |
| Monitor live | `Get-Content ppaf-drill-*.log -Tail 20 -Wait` |
