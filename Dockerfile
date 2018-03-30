FROM centos_systemd

ENV container docker

RUN yum install -y git
RUN mkdir /app
RUN cd /app && mkdir lib

RUN cd /app && git clone https://github.com/dream-lab/echo && cp -r ./echo/echo_platform_service . && cp -r ./echo/misc/lib/* ./lib && rm -rf echo

RUN cd /app && chmod +x echo_platform_service/bootstrap.py && chmod +x echo_platform_service/echo_platform_service.py

RUN yum install -y wget

RUN cd /app && wget https://archive.apache.org/dist/nifi/1.2.0/nifi-1.2.0-bin.tar.gz
RUN cd /app && tar -zxvf nifi-1.2.0-bin.tar.gz 
RUN cd /app && rm nifi-1.2.0-bin.tar.gz && rm -rf nifi-1.2.0/lib/*
RUN cd /app && cp -r ./lib/*  ./nifi-1.2.0/lib/

RUN mkdir /app/data
COPY ./ETL/* /app/data/

RUN yum install -y python-pip
RUN pip install paho-mqtt
RUN yum install -y java-1.8.0-openjdk
RUN yum install -y openssh-clients

