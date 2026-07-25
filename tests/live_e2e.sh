#!/usr/bin/env bash
set -euo pipefail

# Live two-process test. It intentionally uses the configured shared instance,
# so run it only on a host where local Reticulum networking is allowed.
RNS_CONFIG_DIR="${1:-${HOME}/.rsReticulum}"
TEST_HOME="$(mktemp -d /tmp/rrcd-rs-live-e2e.XXXXXX)"
HUB_LOG="${TEST_HOME}/hub.log"
HUB_PID=""
RECONNECT_LOG="${TEST_HOME}/reconnect.log"
RECONNECT_PID=""

cargo build --offline --bin rrcd-rs --bin rrcd-e2e-client
cargo build --offline --manifest-path ../rsRRC-client/Cargo.toml \
    --example live_smoke --example live_reconnect
RSRRCD_HOME="${TEST_HOME}" target/debug/rrcd-rs >/dev/null

stop_hub() {
    if [[ -n "${HUB_PID}" ]]; then
        kill -INT "${HUB_PID}" 2>/dev/null || true
        wait "${HUB_PID}" 2>/dev/null || true
        HUB_PID=""
    fi
}

stop_reconnect_client() {
    if [[ -n "${RECONNECT_PID}" ]]; then
        kill "${RECONNECT_PID}" 2>/dev/null || true
        wait "${RECONNECT_PID}" 2>/dev/null || true
        RECONNECT_PID=""
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

wait_for_reconnect_marker() {
    local marker="$1"
    for _ in $(seq 1 900); do
        if grep -q "${marker}" "${RECONNECT_LOG}"; then
            return
        fi
        if ! kill -0 "${RECONNECT_PID}" 2>/dev/null; then
            cat "${RECONNECT_LOG}" >&2
            exit 1
        fi
        sleep 0.1
    done
    echo "Reconnect client did not report: ${marker}" >&2
    cat "${RECONNECT_LOG}" >&2
    exit 1
}

trap 'stop_reconnect_client; stop_hub' EXIT

start_hub
DESTINATION="$(destination)"
target/debug/rrcd-e2e-client "${DESTINATION}" "${RNS_CONFIG_DIR}" setup
../rsRRC-client/target/debug/examples/live_smoke \
    "${DESTINATION}" "${RNS_CONFIG_DIR}" "client-smoke"

../rsRRC-client/target/debug/examples/live_reconnect \
    "${DESTINATION}" "${RNS_CONFIG_DIR}" 3 >"${RECONNECT_LOG}" 2>&1 &
RECONNECT_PID=$!

for cycle in 1 2 3; do
    wait_for_reconnect_marker "RECONNECT READY ${cycle}"
    stop_hub
    start_hub
    RESTARTED_DESTINATION="$(destination)"
    if [[ "${DESTINATION}" != "${RESTARTED_DESTINATION}" ]]; then
        echo "Hub destination changed across restart" >&2
        exit 1
    fi
    wait_for_reconnect_marker "RECONNECT CYCLE ${cycle}"
done

for _ in $(seq 1 900); do
    if ! kill -0 "${RECONNECT_PID}" 2>/dev/null; then
        break
    fi
    sleep 0.1
done
if kill -0 "${RECONNECT_PID}" 2>/dev/null; then
    echo "Reconnect client did not finish" >&2
    cat "${RECONNECT_LOG}" >&2
    exit 1
fi
if ! wait "${RECONNECT_PID}"; then
    cat "${RECONNECT_LOG}" >&2
    exit 1
fi
RECONNECT_PID=""
cat "${RECONNECT_LOG}"
target/debug/rrcd-e2e-client "${DESTINATION}" "${RNS_CONFIG_DIR}" verify

echo "Live artifacts: ${TEST_HOME}"
