# -*- coding: utf-8 -*-

import os
import staticconf


def load_package_config(filename):
    """Load the contents of a yaml configuration file using
    :func:`staticconf.loader.YamlConfiguration`, and use

    :param filename: filename and path to a yaml config file
    :returns: the contents of ``filename`` as a dict
    """
    return staticconf.YamlConfiguration(filename)


def load_default_config(config_path, env_config_path=None):
    """Load service configuration.

    :param config_path: the path to a yaml config file
    :param env_config_path: the path to a yaml config file with
        overrides on top of 'config_path'
    """
    load_package_config(config_path)
    if env_config_path and os.path.exists(env_config_path):
        load_package_config(env_config_path)
