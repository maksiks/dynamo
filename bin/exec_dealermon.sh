#!/bin/bash

export DYNAMO_BASE=$(dirname $(cd $(dirname ${BASH_SOURCE[0]}); pwd))
source $DYNAMO_BASE/etc/profile.d/init.sh

$DYNAMO_BASE/bin/track_transfers
