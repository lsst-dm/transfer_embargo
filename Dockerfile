# Dockerfile
# A blueprint for building a docker image
FROM python:3.9

# ADD move_embargo_scratch.py .

# RUN setup lsst_distrib -t w_2023_19

FROM lsstsqre/newinstall:latest
USER lsst
RUN source loadLSST.bash && mamba install redis-py rucio-clients
RUN source loadLSST.bash && eups distrib install -t "w_2023_21" obs_lsst

# RUN pip install -r requirements.txt

# CMD ["python", "./move_embargo_scratch.py"]