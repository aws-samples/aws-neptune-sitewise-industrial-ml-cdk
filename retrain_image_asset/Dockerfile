FROM nvidia/cuda:11.0.3-base-ubuntu20.04

WORKDIR /usr/src/app

RUN apt-get update
RUN apt-get install python3-pip -y

RUN pip3 install pandas==1.5.2 \
    boto3==1.24.15 \
    fsspec==2022.10.0 \
    s3fs==2022.10.0 

COPY training.py .
RUN mkdir refdata

HEALTHCHECK NONE

CMD ["python3", "-u", "training.py"]
