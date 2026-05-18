#!/usr/bin/env bash
#
# Builds azure-cosmos JAR from a specified branch/fork and installs it to local Maven repository.
#
# Usage:
#   ./build-custom-cosmos.sh --repo jeet1995/azure-sdk-for-java --branch AzCosmos_WriteAvailabilityStrategyForPPAF
#   ./build-custom-cosmos.sh --branch main
#   ./build-custom-cosmos.sh --repo jeet1995/azure-sdk-for-java --branch AzCosmos_WriteAvailabilityStrategyForPPAF --skip-clone

set -euo pipefail

REPO="Azure/azure-sdk-for-java"
BRANCH="main"
CLONE_DIR=".cosmos-build"
SKIP_CLONE=false
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

while [[ $# -gt 0 ]]; do
    case $1 in
        --repo) REPO="$2"; shift 2 ;;
        --branch) BRANCH="$2"; shift 2 ;;
        --clone-dir) CLONE_DIR="$2"; shift 2 ;;
        --skip-clone) SKIP_CLONE=true; shift ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

REPO_URL="https://github.com/${REPO}.git"
FULL_CLONE_PATH="${SCRIPT_DIR}/${CLONE_DIR}"

echo "=== Azure Cosmos Custom JAR Builder ==="
echo "  Repository : ${REPO}"
echo "  Branch     : ${BRANCH}"
echo "  Clone Dir  : ${FULL_CLONE_PATH}"
echo ""

# Step 1: Clone or fetch
if [ "$SKIP_CLONE" = true ] && [ -d "$FULL_CLONE_PATH" ]; then
    echo "[1/3] Fetching latest changes..."
    cd "$FULL_CLONE_PATH"
    git fetch origin "$BRANCH"
    git checkout "$BRANCH"
    git reset --hard "origin/${BRANCH}"
    cd "$SCRIPT_DIR"
else
    if [ -d "$FULL_CLONE_PATH" ]; then
        echo "[1/3] Removing existing clone directory..."
        rm -rf "$FULL_CLONE_PATH"
    fi
    echo "[1/3] Shallow-cloning ${REPO_URL} (branch: ${BRANCH})..."
    git clone --depth 1 --branch "$BRANCH" --single-branch "$REPO_URL" "$FULL_CLONE_PATH"
fi

# Step 2: Build azure-cosmos and install to local repo
echo ""
echo "[2/3] Building azure-cosmos module..."
cd "$FULL_CLONE_PATH"
mvn install -pl sdk/cosmos/azure-cosmos -am -DskipTests -Dgpg.skip -Dcheckstyle.skip -Dspotbugs.skip -Drevapi.skip -Dmaven.javadoc.skip=true -T 2C
cd "$SCRIPT_DIR"

# Step 3: Extract the built version
echo ""
echo "[3/3] Extracting built version..."
BUILT_VERSION=$(mvn help:evaluate -pl sdk/cosmos/azure-cosmos -Dexpression=project.version -q -DforceStdout -f "${FULL_CLONE_PATH}/pom.xml" 2>/dev/null || echo "unknown")

echo ""
echo "=== Build Complete ==="
echo "  Built version: ${BUILT_VERSION}"
echo ""
echo "To use this JAR with ppaf-dr-drill-workload, run:"
echo "  mvn clean package -Dpackage-with-dependencies -Plocal-cosmos -Dcosmos.version=${BUILT_VERSION}"
echo ""
echo "Or to just build without fat JAR:"
echo "  mvn clean compile -Plocal-cosmos -Dcosmos.version=${BUILT_VERSION}"
