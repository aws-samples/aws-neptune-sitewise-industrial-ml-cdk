FROM public.ecr.aws/lambda/python:3.9
USER me

# You can specify custom Dockerfile commands to run at build time e.g:
#
#	$ docker build --build-arg EXTRA_CMDS='echo hello' -t .
#
# This is useful if you need additional prerequisites for the lambda function.
ARG EXTRA_CMDS
ENV EXTRA_CMDS=${EXTRA_CMDS:-"echo 'NO EXTRA COMMANDS'"}

RUN yum update -y \
    && yum install -y shadow-utils.x86_64 zip unzip \
    gcc-c++ make openssl-devel zlib-devel readline-devel git

RUN /bin/bash -c "${EXTRA_CMDS}"
RUN pip3 install stable_baselines3==1.5.0 \
    psychrolib==2.5.0

RUN pip3 --no-cache-dir install --upgrade awscli

COPY infer_lambda.py ${LAMBDA_TASK_ROOT}
COPY model.csv ${LAMBDA_TASK_ROOT}

HEALTHCHECK NONE

CMD ["infer_lambda.handler"]
