<#
.SYNOPSIS
    Builds azure-cosmos JAR from a specified branch/fork and installs it to local Maven repository.

.DESCRIPTION
    Clones (shallow) or fetches the specified branch from the azure-sdk-for-java repository,
    builds the azure-cosmos module, and installs it to the local Maven repository.
    The ppaf-dr-drill-workload can then use the local-cosmos Maven profile to pick it up.

.PARAMETER Repo
    The GitHub repository in 'owner/repo' format. Default: 'Azure/azure-sdk-for-java'

.PARAMETER Branch
    The branch to build from. Default: 'main'

.PARAMETER CloneDir
    Directory to clone into. Default: '.cosmos-build' in the current directory.

.PARAMETER SkipClone
    Skip cloning if the directory already exists and just fetch/checkout.

.EXAMPLE
    # Build from the PPAF write availability strategy PR branch
    .\build-custom-cosmos.ps1 -Repo "jeet1995/azure-sdk-for-java" -Branch "AzCosmos_WriteAvailabilityStrategyForPPAF"

.EXAMPLE
    # Build from Azure main
    .\build-custom-cosmos.ps1 -Branch "main"

.EXAMPLE
    # Re-build after pulling latest changes (skip clone)
    .\build-custom-cosmos.ps1 -Repo "jeet1995/azure-sdk-for-java" -Branch "AzCosmos_WriteAvailabilityStrategyForPPAF" -SkipClone
#>

param(
    [string]$Repo = "Azure/azure-sdk-for-java",
    [string]$Branch = "main",
    [string]$CloneDir = ".cosmos-build",
    [switch]$SkipClone
)

$ErrorActionPreference = "Stop"

$repoUrl = "https://github.com/$Repo.git"
$fullClonePath = Join-Path $PSScriptRoot $CloneDir

Write-Host "=== Azure Cosmos Custom JAR Builder ===" -ForegroundColor Cyan
Write-Host "  Repository : $Repo"
Write-Host "  Branch     : $Branch"
Write-Host "  Clone Dir  : $fullClonePath"
Write-Host ""

# Step 1: Clone or fetch
if ($SkipClone -and (Test-Path $fullClonePath)) {
    Write-Host "[1/3] Fetching latest changes..." -ForegroundColor Yellow
    Push-Location $fullClonePath
    try {
        git fetch origin $Branch
        git checkout $Branch
        git reset --hard "origin/$Branch"
    } finally {
        Pop-Location
    }
} else {
    if (Test-Path $fullClonePath) {
        Write-Host "[1/3] Removing existing clone directory..." -ForegroundColor Yellow
        Remove-Item -Recurse -Force $fullClonePath
    }
    Write-Host "[1/3] Shallow-cloning $repoUrl (branch: $Branch)..." -ForegroundColor Yellow
    git clone --depth 1 --branch $Branch --single-branch $repoUrl $fullClonePath
    if ($LASTEXITCODE -ne 0) {
        Write-Error "Failed to clone repository"
        exit 1
    }
}

# Step 2: Build azure-cosmos and install to local repo
Write-Host ""
Write-Host "[2/3] Building azure-cosmos module..." -ForegroundColor Yellow
Push-Location $fullClonePath
try {
    # Build azure-cosmos with its required dependencies, skip tests for speed
    mvn install -pl sdk/cosmos/azure-cosmos -am -DskipTests -Dgpg.skip -Dcheckstyle.skip -Dspotbugs.skip -Drevapi.skip -Dmaven.javadoc.skip=true -T 2C
    if ($LASTEXITCODE -ne 0) {
        Write-Error "Maven build failed"
        exit 1
    }
} finally {
    Pop-Location
}

# Step 3: Extract the version that was built
Write-Host ""
Write-Host "[3/3] Extracting built version..." -ForegroundColor Yellow
$pomPath = Join-Path $fullClonePath "sdk/cosmos/azure-cosmos/pom.xml"
[xml]$pom = Get-Content $pomPath
$builtVersion = $pom.project.version
if (-not $builtVersion) {
    # Version might be inherited from parent
    [xml]$parentPom = Get-Content (Join-Path $fullClonePath "sdk/cosmos/pom.xml")
    $builtVersion = $parentPom.project.version
}

Write-Host ""
Write-Host "=== Build Complete ===" -ForegroundColor Green
Write-Host "  Built version: $builtVersion"
Write-Host ""
Write-Host "To use this JAR with ppaf-dr-drill-workload, run:" -ForegroundColor Cyan
Write-Host "  mvn clean package -Dpackage-with-dependencies -Plocal-cosmos -Dcosmos.version=$builtVersion" -ForegroundColor White
Write-Host ""
Write-Host "Or to just build without fat JAR:" -ForegroundColor Cyan
Write-Host "  mvn clean compile -Plocal-cosmos -Dcosmos.version=$builtVersion" -ForegroundColor White
