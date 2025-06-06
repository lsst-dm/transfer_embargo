#!/bin/bash

# Input environment variables:
# SWDIR = directory where software is located
# DRY_RUN = (optional) "--dry_run" to indicate not to affect the destination
# NOW = (optional) "--now ISOT" argument with past datetime in isot format to consider as the current time
# TRANSFER_CONFIG = path to transfer policy configuration
# WINDOW = time window to scan for eligible files, previous to $NOW, as "NNmin" or "NNhr"
# DEST = destination directory for raw zips
# RUCIO = (optional) arguments for Rucio RSE and scope
# FROMREPO = source Butler repo
# TOREPO = destination Butler repo

# Prevent the world from accessing outputs.
umask 027

# Ensure a home directory is present for eups.
export HOME="$TMPDIR"/"$LOGNAME"
mkdir -p "$HOME"

# Ensure SHELL is set for conda.
export SHELL=/bin/bash

source /opt/lsst/software/stack/loadLSST.sh
setup lsst_distrib
# DRY_RUN and NOW may be empty, so do not quote them.
# RUCIO may hold multiple options, so do not quote it.
echo python "$SWDIR"/transfer_zip.py \
    $DRY_RUN \
    $NOW \
    -C "$TRANSFER_CONFIG" \
    --window "$WINDOW" \
    -d "$DEST" \
    $RUCIO \
    "$FROMREPO" "$TOREPO"
python "$SWDIR"/transfer_zip.py \
    $DRY_RUN \
    $NOW \
    -C "$TRANSFER_CONFIG" \
    --window "$WINDOW" \
    -d "$DEST" \
    $RUCIO \
    "$FROMREPO" "$TOREPO" \
    2>&1 |
    if [ -d "$LOGDIR" ]; then
        mkdir -p "$LOGDIR"/$(date +\%Y-\%m)/$(date -I)
        tee "$LOGDIR/$(date +\%Y-\%m)/$(date -I)/$(date -Im)@$WINDOW".log
    else
        cat
    fi
