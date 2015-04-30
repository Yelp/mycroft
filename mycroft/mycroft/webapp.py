# -*- coding: utf-8 -*-

#import pyramid_uwsgi_metrics
import uwsgi_metrics
from pyramid.config import Configurator

import mycroft.config


uwsgi_metrics.initialize()


def _create_application():
    """Create the WSGI application, post-fork."""
    mycroft.config.load_staticconf()
    # Initialize configs at the early stage to avoid repeated calculation at runtime.
    mycroft.config.init()

    # Create a basic pyramid Configurator.
    config = Configurator(settings={
        'service_name': 'mycroft',
    })

    # Add the service's custom configuration, routes, etc.
    config.include(mycroft.config.routes)
    config.include(mycroft.config.swagger_views)
    config.include(mycroft.config.renderers)

    config.add_route('status.healthcheck', '/status/healthcheck')

    # Display metrics on the '/status/metrics' endpoint
    # config.include(pyramid_uwsgi_metrics)

    # Scan the service package to attach any decorated views.
    config.scan('mycroft')

    return config.make_wsgi_app()


def create_application():
    return _create_application()
