# Dockerfile
FROM python:3.11

# Copy source code and test files
COPY requirements.txt /opt/lsst/transfer_embargo/
COPY src/ /opt/lsst/transfer_embargo/src/
COPY tests_docker/ /opt/lsst/transfer_embargo/tests_docker/

# Set the working directory
WORKDIR /opt/lsst/transfer_embargo

# List files for debugging
# RUN ls -la /opt/lsst/transfer_embargo/
# RUN ls -R /opt/lsst/transfer_embargo/src/
# RUN ls -R /opt/lsst/transfer_embargo/tests_docker/
# RUN ls -R /opt/lsst/transfer_embargo/tests/data/test_from/

RUN pip install -r requirements.txt

# Define the environment variables
# These are written over if they are re-defined
# by the cronjob or on the command line deploy
# of the pod
ENV FROMREPO "tests_docker/temp_from"
ENV TOREPO "tests_docker/temp_to"
ENV INSTRUMENT "LATISS"
ENV NOW "2020-03-01 23:59:59.999999"
ENV EMBARGO_HRS "1063.08018813861"
ENV DATASETTYPE "raw"
#'["raw", "calexp"]'
ENV COLLECTIONS "LATISS/raw/all"
#'["LATISS/raw/all", "LATISS/runs/AUXTEL_DRP_IMAGING_2022-11A/w_2022_46/PREOPS-1616"]'
ENV LOG "True"
ENV PASTEMBARGO "1.0"

#CMD ["/bin/sh", "-c", "python src/move_embargo_args.py \"$FROMREPO\" \"$TOREPO\" \"$INSTRUMENT\" --log \"$LOG\""]

CMD ["/bin/sh", "-c", "python src/move_embargo_args.py \"$FROMREPO\" \"$TOREPO\" \"$INSTRUMENT\" --nowtime \"$NOW\" --embargohours \"$EMBARGO_HRS\" --log \"$LOG\" --pastembargohours \"$PASTEMBARGO\" --datasettype $DATASETTYPE --collections $COLLECTIONS"]
