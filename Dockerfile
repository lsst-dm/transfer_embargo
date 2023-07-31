# Dockerfile
COPY ./src /opt/lsst/transfer_embargo
WORKDIR /opt/lsst/transfer_embargo

FROM python:3.9

# ADD move_embargo_scratch.py .

# RUN setup lsst_distrib -t w_2023_19



FROM lsstsqre/newinstall:latest
USER lsst
RUN source loadLSST.bash && mamba install rucio-clients
RUN source loadLSST.bash && eups distrib install -t "w_2023_21" obs_lsst

# RUN pip install -r requirements.txt

CMD ["python", "-m", "ensurepip"]
CMD ["python", "-m", "pip", "install", "lsst-daf-butler"]
# this is from the test.yml file
#python -m ensurepip
#python -m pip install lsst-daf-butler

# CMD ["python", "./move_embargo_scratch.py"]