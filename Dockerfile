# Dockerfile
ARG RUBINENV_VERSION=9.0.0
FROM ghcr.io/lsst-dm/docker-newinstall:9-latest-${RUBINENV_VERSION}
# workdir is /opt/lsst/software/stack

# Copy source code and test files
COPY requirements.txt /opt/lsst/transfer_embargo/
COPY src/ /opt/lsst/transfer_embargo/src/
COPY tests_docker/ /opt/lsst/transfer_embargo/tests_docker/

# Set the working directory
WORKDIR /opt/lsst/transfer_embargo

# List files for debugging
RUN ls -la /opt/lsst/transfer_embargo/
# RUN ls -R /opt/lsst/transfer_embargo/src/
# RUN ls -R /opt/lsst/transfer_embargo/tests_docker/
# RUN ls -R /opt/lsst/transfer_embargo/tests/data/test_from/

# RUN pip install -r requirements.txt
ARG OBS_LSST_VERSION
ENV OBS_LSST_VERSION=${OBS_LSST_VERSION:-w_2024_32}
# USER lsst

# debug eups
# RUN command -v eups || echo "eups not found in PATH"

# trying to explicitly run in a bash shell
#RUN bash -c "source loadLSST.bash && eups distrib install -t \"${OBS_LSST_VERSION}\" obs_lsst"
RUN <<EOF
set -e
source /opt/lsst/software/stack/loadLSST.bash
command -v eups || echo "eups not found in PATH"
eups distrib install -t "${OBS_LSST_VERSION}" obs_lsst
EOF

RUN command -v bash || echo "bash not found"


# Define the environment variables
# These are written over if they are re-defined
# by the cronjob or on the command line deploy
# of the pod
ENV FROMREPO "tests_docker/temp_from"
ENV TOREPO "tests_docker/temp_to"
ENV INSTRUMENT "LATISS"
ENV DATAQUERIES "--dataqueries '{ \"datasettype\": \"raw\", \"collections\": \"LATISS/raw/all\"}'"
ENV LOG "True"
ENV PASTEMBARGO "1.0"
ENV OTHER_ARGUMENTS "--embargohours 80 --nowtime \"now\""

#CMD ["/bin/sh", "-c", "python src/move_embargo_args.py \"$FROMREPO\" \"$TOREPO\" \"$INSTRUMENT\" --log \"$LOG\""]

#CMD ["/bin/sh", "-c", "python src/move_embargo_args.py \"$FROMREPO\" \"$TOREPO\" \"$INSTRUMENT\" --log \"$LOG\" --pastembargohours \"$PASTEMBARGO\" $DATAQUERIES $OTHER_ARGUMENTS"]

ENTRYPOINT [ "sh", "-c", "source /opt/lsst/software/stack/loadLSST.bash; setup lsst_obs; python src/move_embargo_args.py \"$FROMREPO\" \"$TOREPO\" \"$INSTRUMENT\" --log \"$LOG\" --pastembargohours \"$PASTEMBARGO\" $DATAQUERIES $OTHER_ARGUMENTS" ]
