FROM ubuntu:14.04.1
MAINTAINER bam@yelp.com

FROM mycroft

RUN apt-get update && apt-get install -y openjdk-7-jre

ADD ./mycroft/requirements-dev.txt /mycroft/requirements-dev.txt

RUN pip -v install --exists-action=w -r ./requirements-dev.txt
