#!/usr/bin/env bash
set -euo pipefail

# Live two-process test. It intentionally uses the configured shared instance,
# so run it only on a host where local Reticulum networking is allowed.
RNS_CONFIG_DIR="${1:-${HOME}/.rsReticulum}"
TEST_HOME="$(mktemp -d /tmp/rrcd-rs-live-e2e.XXXXXX)"
HUB_LOG="${TEST_HOME}/hub.log"
HUB_PID=""

cargo build --offline --bin rrcd-rs --bin rrcd-e2e-client
RSRRCD_HOME="${TEST_HOME}" target/debug/rrcd-rs >/dev/null

stop_hub() {
    if [[ -n "${HUB_PID}" ]]; then
        kill -INT "${HUB_PID}" 2>/dev/null || true
        wait "${HUB_PID}" 2>/dev/null || true
        HUB_PID=""
    fi
}

start_hub() {
    : >"${HUB_LOG}"
    RSRRCD_HOME="${TEST_HOME}" target/debug/rrcd-rs >"${HUB_LOG}" 2>&1 &
    HUB_PID=$!
    for _ in $(seq 1 100); do
        if grep -q 'rrcd-rs destination <' "${HUB_LOG}"; then
            return
        fi
        sleep 0.1
    done
    echo "Hub did not publish a destination. Log: ${HUB_LOG}" >&2
    exit 1
}

destination() {
    sed -n 's/.*rrcd-rs destination <\([0-9a-f]*\)>.*/\1/p' "${HUB_LOG}" |
        head -n 1
}

trap stop_hub EXIT

start_hub
DESTINATION="$(destination)"
target/debug/rrcd-e2e-client "${DESTINATION}" "${RNS_CONFIG_DIR}" setup

stop_hub
start_hub
RESTARTED_DESTINATION="$(destination)"
if [[ "${DESTINATION}" != "${RESTARTED_DESTINATION}" ]]; then
    echo "Hub destination changed across restart" >&2
    exit 1
fi
target/debug/rrcd-e2e-client "${DESTINATION}" "${RNS_CONFIG_DIR}" verify

echo "Live artifacts: ${TEST_HOME}"
