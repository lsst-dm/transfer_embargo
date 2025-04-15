#!/bin/bash

# Prevent the world from accessing outputs.
umask 027

# Ensure a home directory is present for eups.
export HOME="$TMPDIR"/"$LOGNAME"
mkdir -p "$HOME"

# Ensure SHELL is set for conda.
export SHELL=/bin/bash

source /opt/lsst/software/stack/loadLSST.sh
setup lsst_distrib
# RUCIO may hold multiple options, so do not quote it.
python /opt/lsst/transfer_embargo/transfer_zip.py \
    "$DRY_RUN" \
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
