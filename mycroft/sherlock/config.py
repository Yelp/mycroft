# -*- coding: utf-8 -*-
import logging


log = logging.getLogger('sherlock.config')


def routes(config):
    """Example configuration function that adds a single endpoint."""
    config.add_route('api.hello', '/hello/{name}')
