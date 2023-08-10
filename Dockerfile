# Dockerfile

FROM lsstsqre/newinstall:latest
USER lsst

COPY ./src /opt/lsst/transfer_embargo
WORKDIR /opt/lsst/transfer_embargo

FROM python:3.9

# ADD move_embargo_scratch.py .

# RUN setup lsst_distrib -t w_2023_19



RUN source loadLSST.bash && mamba install rucio-clients
RUN source loadLSST.bash && eups distrib install -t "w_2023_21" obs_lsst

# RUN pip install -r requirements.txt

CMD ["python", "-m", "ensurepip"]
CMD ["python", "-m", "pip", "install", "lsst-daf-butler"]
# this is from the test.yml file
#python -m ensurepip
#python -m pip install lsst-daf-butler


# Define the environment variables
ARG FROMREPO="/repo/embargo"
ARG TOREPO="/repo/main"
RUN echo "The fromrepo value is $FROMREPO, the torepo value is $TOREPO"
ARG INSTRUMENT="LATISS"
ARG EMBARGO_HRS=80
ARG MOVE="True"

CMD ["python", "./move_embargo_scratch.py", FROMREPO, TOREPO, INSTRUMENT, "--embargohours", EMBARGO_HRS, "--move", MOVE]
#["python", "../src/move_embargo_args.py", temp_from, temp_to,
#"LATISS","--embargohours", str(embargo_hours),
#"--datasettype","raw",
#"--collections","LATISS/raw/all",
#"--nowtime",now_time_embargo,
#"--move",move,
#"--log",log]