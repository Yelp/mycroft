"""WSGI server configuration file

A callable named application MUST be defined by this file which will be run
by the WSGI server.

Anything that is specific to the WSGI server of your choice should be done
here. If you need to pass something through, pass it through
create_application.
"""

from sherlock.common.util import memoized

from mycroft.webapp import create_application

# Memoizing the application to prevent double-init on packages
memoized_create_application = memoized(create_application)


def application(environ, start_response):
    return memoized_create_application()(environ, start_response)
