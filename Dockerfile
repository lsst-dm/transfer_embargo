# Dockerfile
# A blueprint for building a docker image
FROM python:3.9

# ADD move_embargo_scratch.py .

RUN setup lsst_distrib -t w_2023_19

# RUN pip install -r requirements.txt

# CMD ["python", "./move_embargo_scratch.py"]