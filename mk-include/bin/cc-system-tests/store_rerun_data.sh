#!/bin/bash

echo "store time"

[ "$DEBUG" = true ] && set -x
set -e

BUILD_DIR="${BUILD_DIR:-$SEMAPHORE_GIT_DIR/build}"


if [ -d "${BUILD_DIR}/rerun-data" ]; then
    echo "storing data in ${SEMAPHORE_WORKFLOW_ID}-${SEMAPHORE_JOB_NAME}-rerun-data"
    cache store "${SEMAPHORE_WORKFLOW_ID}-${SEMAPHORE_JOB_NAME}-rerun-data" "${BUILD_DIR}/rerun-data"
    if [ -f "${BUILD_DIR}/rerun-data/failed_test.txt" ]; then
        echo "found failed tests:"
        cat "${BUILD_DIR}/rerun-data/failed_test.txt"
        echo ""
        artifact push job "${BUILD_DIR}/rerun-data/failed_test.txt" -d rerun-data/failed_test.txt
    else
        echo "failed tests not found"
    fi
else
    echo "rerun data not found: ${BUILD_DIR}/rerun-data"
fi
