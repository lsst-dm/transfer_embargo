# Dockerfile

#FROM lsstsqre/newinstall:latest
#USER lsst

FROM python:3.11

#COPY src/ /opt/lsst/transfer_embargo
#COPY tests_docker/ /opt/lsst/transfer_embargo
#COPY tests/data/test_from/ /opt/lsst/transfer_embargo
#WORKDIR /opt/lsst/transfer_embargo

#RUN ls -la /opt/lsst/transfer_embargo/
#RUN ls -R /opt/lsst/transfer_embargo/LATISS/


# Copy source code and test files
COPY src/ /opt/lsst/transfer_embargo/src/
COPY tests_docker/ /opt/lsst/transfer_embargo/tests_docker/
COPY tests/data/test_from/ /opt/lsst/transfer_embargo/tests/data/test_from/

# Set the working directory
WORKDIR /opt/lsst/transfer_embargo

# List files for debugging
RUN ls -la /opt/lsst/transfer_embargo/
RUN ls -R /opt/lsst/transfer_embargo/src/
RUN ls -R /opt/lsst/transfer_embargo/tests_docker/
RUN ls -R /opt/lsst/transfer_embargo/tests/data/test_from/




# ADD move_embargo_scratch.py .

# RUN setup lsst_distrib -t w_2023_19

RUN pip install lsst-daf-butler  


# RUN source loadLSST.bash && mamba install rucio-clients
# RUN source loadLSST.bash && eups distrib install -t "w_2023_21" obs_lsst

# RUN pip install -r requirements.txt

# this is from the test.yml file
# python -m ensurepip
# python -m pip install lsst-daf-butler

# ADD ./src/move_embargo_args.py .

ENV FROMREPO "tests_docker/temp_from"
ENV TOREPO "tests_docker/temp_to"

RUN echo "The fromrepo value is $FROMREPO, the torepo value is $TOREPO"

#ENV FROMREPO $FROMREPO
#ENV FROMREPO $FROMREPO

# Define the environment variables
ENV INSTRUMENT "LATISS"
ENV EMBARGO_HRS "80"
ENV MOVE "True"



# tests_docker currently only has the create_testto_butler.sh
# file in it and needs to have all fo the files from
# the python testing (transfer_embargo/tests/data/test_from/)

# Create necessary directories and run commands
CMD ["/bin/sh", "-c", "mkdir -p $FROMREPO/LATISS $TOREPO; cp -r tests/data/test_from/* $FROMREPO/; chmod u+x tests_docker/create_testto_butler.sh; ./tests_docker/create_testto_butler.sh $TOREPO; python src/move_embargo_args.py $FROMREPO $TOREPO $INSTRUMENT --embargohours $EMBARGO_HRS --move $MOVE"]


RUN ls -R /opt/lsst/transfer_embargo/tests_docker/

#CMD ["/bin/sh", "-c", "mkdir $FROMREPO $TOREPO; mkdir $FROMREPO/LATISS; cp -r LATISS/* $FROMREPO/LATISS/; cp butler.yaml $FROMREPO; cp gen3.sqlite3 $FROMREPO; chmod u+x create_testto_butler.sh; create_testto_butler.sh $TOREPO; python move_embargo_args.py $FROMREPO $TOREPO $INSTRUMENT --embargohours $EMBARGO_HRS --move $MOVE"]

# CMD ["python", "./src/move_embargo_args.py", FROMREPO, TOREPO, INSTRUMENT, "--embargohours", EMBARGO_HRS, "--move", MOVE]
# ["python", "../src/move_embargo_args.py", temp_from, temp_to,
# "LATISS","--embargohours", str(embargo_hours),
# "--datasettype","raw",
# "--collections","LATISS/raw/all",
# "--nowtime",now_time_embargo,
# "--move",move,
# "--log",log]
