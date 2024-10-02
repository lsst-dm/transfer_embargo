## Dockerfile
ARG RUBINENV_VERSION=9.0.0
FROM ghcr.io/lsst-dm/docker-newinstall:9-latest-${RUBINENV_VERSION}
## workdir is /opt/lsst/software/stack

## Copy source code and test files
COPY requirements.txt /opt/lsst/transfer_embargo/
COPY src/ /opt/lsst/transfer_embargo/src/
COPY tests_docker/ /opt/lsst/transfer_embargo/tests_docker/

## Set the working directory
WORKDIR /opt/lsst/transfer_embargo

## Optionally list files for debugging
# RUN ls -la /opt/lsst/transfer_embargo/

ARG OBS_LSST_VERSION
ENV OBS_LSST_VERSION=${OBS_LSST_VERSION:-w_2024_32}

## Optionally debug eups
# RUN command -v eups || echo "eups not found in PATH"

## Optionally check that bash is available and loadLSST.bash works
# RUN ls -la /opt/lsst/software/stack/loadLSST.bash
# RUN command -v bash || echo "bash not found, installing bash"

## Run in a bash shell
RUN <<EOF
set -e
source /opt/lsst/software/stack/loadLSST.bash
command -v eups || echo "eups not found in PATH"
eups distrib install -t "${OBS_LSST_VERSION}" obs_lsst
EOF

## Define the environment variables
## These are written over if they are re-defined
## by the pod deployment yaml or cli
ENV FROMREPO "tests_docker/temp_from"
ENV TOREPO "tests_docker/temp_to"
ENV INSTRUMENT "LATISS"
ENV DATAQUERIES "--dataqueries '{ \"datasettype\": \"raw\", \"collections\": \"LATISS/raw/all\"}'"
ENV PASTEMBARGO "1.0"
ENV OTHER_ARGUMENTS "--embargohours 80 --nowtime now"

ENTRYPOINT [ "bash", "-c", "source /opt/lsst/software/stack/loadLSST.bash; setup lsst_obs; python src/move_embargo_args.py \"$FROMREPO\" \"$TOREPO\" \"$INSTRUMENT\" --log \"$LOG\" --pastembargohours \"$PASTEMBARGO\" $DATAQUERIES $OTHER_ARGUMENTS" ]
