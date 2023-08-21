# Dockerfile

#FROM lsstsqre/newinstall:latest
#USER lsst

FROM python:3.11

COPY ./src /opt/lsst/transfer_embargo
WORKDIR /opt/lsst/transfer_embargo

# ADD move_embargo_scratch.py .

# RUN setup lsst_distrib -t w_2023_19

RUN pip install lsst-daf-butler  


# RUN source loadLSST.bash && mamba install rucio-clients
# RUN source loadLSST.bash && eups distrib install -t "w_2023_21" obs_lsst

# RUN pip install -r requirements.txt

# CMD ["python", "-m", "ensurepip"]
# CMD ["python", "-m", "pip", "install", "lsst-daf-butler"]
# this is from the test.yml file
# python -m ensurepip
# python -m pip install lsst-daf-butler

# ADD ./src/move_embargo_args.py .

ARG FROMREPO="./tests_docker/temp_from"
ARG TOREPO="./tests_docker/temp_to"

RUN echo "The fromrepo value is $FROMREPO, the torepo value is $TOREPO"

ENV FROMREPO $FROMREPO
ENV FROMREPO $FROMREPO

# Define the environment variables
ARG INSTRUMENT="LATISS"
ARG EMBARGO_HRS="80"
ARG MOVE="True"

ENV INSTRUMENT $INSTRUMENT
ENV EMBARGO_HRS $EMBARGO_HRS
ENV MOVE $MOVE

CMD ["/bin/sh", "-c", "mkdir $FROMREPO $TOREPO; cp -r ../tests/data/test_from $FROMREPO; chmod u+x ./tests_docker/create_testto_butler.sh; ./tests_docker/create_testto_butler.sh $TOREPO; python ./src/move_embargo_args.py $FROMREPO $TOREPO $INSTRUMENT --embargohours $EMBARGO_HRS --move $MOVE"]

# CMD ["python", "./src/move_embargo_args.py", FROMREPO, TOREPO, INSTRUMENT, "--embargohours", EMBARGO_HRS, "--move", MOVE]
# ["python", "../src/move_embargo_args.py", temp_from, temp_to,
# "LATISS","--embargohours", str(embargo_hours),
# "--datasettype","raw",
# "--collections","LATISS/raw/all",
# "--nowtime",now_time_embargo,
# "--move",move,
# "--log",log]

# CMD ["/bin/sh", "-c", "rm -rf $FROMREPO $TOREPO"]