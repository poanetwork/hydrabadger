#!/bin/bash

# Starts a Hydrabadger node
# =========================
#
# Optional environment variables:
#
# * HYDRABADGER_LOG_ADDTL:
#
#   Appends additional logging args (debug, trace, filters, etc.).
#
# * HYDRABADGER_RELEASE:
#
#   Builds in debug mode if `HYDRABADGER_RELEASE` == `0` or `false`.
#   Slows down output and makes reading log easier.
#
#

let HOST_ID=$1
let PEER_0_ID=$HOST_ID==0?$HOST_ID+2:$HOST_ID-1
let PEER_1_ID=$HOST_ID+1

printf -v HOST_PORT "3%03d" $HOST_ID
printf -v PEER_0_PORT "3%03d" $PEER_0_ID
printf -v PEER_1_PORT "3%03d" $PEER_1_ID


if [[ $HYDRABADGER_LOG ]]
then
    HYDRABADGER_LOG=$HYDRABADGER_LOG
else
    HYDRABADGER_LOG="info"
fi

if [[ $HYDRABADGER_RELEASE ]]
then
    case "$HYDRABADGER_RELEASE" in
        0|false) RELEASE=""; DIRECTORY="debug" ;;
        *) RELEASE="--release"; DIRECTORY="release" ;;
    esac
else
    RELEASE="--release"
    DIRECTORY="release"
fi

set -e

cargo build $RELEASE

# printf "\nLimiting process memory limit to 400MiB...\n\n"
# ulimit -Sv 400000

bash -c "\
HYDRABADGER_LOG=$HYDRABADGER_LOG target/$DIRECTORY/peer_node \
    -b localhost:$HOST_PORT \
    -r localhost:$PEER_0_PORT \
    -r localhost:$PEER_1_PORT\
    $2 $3 $4 $5 $6 $7 $8
"
