FROM ubuntu:14.04.1
MAINTAINER bam@yelp.com

ENV DEBIAN_FRONTEND noninteractive

RUN apt-get update && apt-get upgrade -y
RUN apt-get install -y wget language-pack-en-base

RUN locale-gen en_US en_US.UTF-8 && dpkg-reconfigure locales

RUN mkdir /src
WORKDIR /src

RUN apt-get update && apt-get install -y \
    python-pkg-resources \
    python-setuptools \
    python-virtualenv \
    python-pip \
    build-essential \
    python-dev \
    git \
    vim \
    libmysqlclient-dev \
    libpq5 \
    libpq-dev \
    libsnappy1 \
    libsnappy-dev \
    cython \
    libssl0.9.8 \
    m4

RUN apt-get install -y \
    libyaml-0-2 \
    libxml2 \
    libpython2.7

RUN ln -s /usr/bin/gcc /usr/local/bin/cc

RUN mkdir /mycroft
WORKDIR /mycroft

# Adding requirments here, so we only reinstall requirements if they change
ADD ./mycroft/requirements.txt /mycroft/requirements.txt
ADD ./mycroft/requirements-custom.txt /mycroft/requirements-custom.txt
ADD ./mycroft/requirements-emr.txt /mycroft/requirements-emr.txt

RUN pip -v install --exists-action=w -r ./requirements.txt

ADD ./mycroft /mycroft
RUN pip -v install -e /mycroft

ENV MYCROFTCODE /mycroft
ENV YELPCODE /mycroft
ENV BASEPATH /mycroft
ENV LOGNAME mycroft

RUN mkdir -p logs
RUN make clean
RUN make emr

# Add the config last - we don't want to rebuild everything if only this changes
ADD ./mycroft_config /mycroft_config

EXPOSE 13819 13820
