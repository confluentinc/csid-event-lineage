#!/bin/bash

BUILD_DIR="${BUILD_DIR:-$SEMAPHORE_GIT_DIR/build}"
RERUN_DIR="${RERUN_DIR:-$BUILD_DIR/rerun-data}"

cache restore "${SEMAPHORE_WORKFLOW_ID}-${SEMAPHORE_JOB_NAME}-rerun-data"
if [ -f "${RERUN_DIR}/failed_test.txt" ]; then
    export RERUN_TESTS="$(cat ${BUILD_DIR}/rerun-data/failed_test.txt)"
    if [ -n "$RERUN_TESTS" ]; then 
        echo "rerunning: $RERUN_TESTS"
    else
        echo "no tests to rerun found, using originally configured value"
    fi
else
    echo "file '${RERUN_DIR}/failed_test.txt' not found"
fi
cache delete "${SEMAPHORE_WORKFLOW_ID}-rerun-data"

# reset status of rerun_dir to be cleared out to be written to durring test execution
rm -rf "${RERUN_DIR}"
mkdir -p "${RERUN_DIR}"
