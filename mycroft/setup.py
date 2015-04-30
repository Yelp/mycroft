#!/usr/bin/env python
# -*- coding: utf-8 -*-

from setuptools import setup, find_packages

setup(
    name='mycroft',
    license='MIT',
    maintainer='Yelp, Inc',
    maintainer_email='bam@yelp.com',
    description='Pipeline for ingesting logs into Redshift ',
    url='https://gitweb.github.com/?p=services/mycroft.git',
    packages=find_packages(),
    setup_requires=['setuptools'],
    version='0.1.0',
    # This list is for the libraries you need to run your application.
    install_requires=[
        'avro',
        'uwsgi',
        'pyramid',
#        'pyramid_uwsgi_metrics',
    ]
)
