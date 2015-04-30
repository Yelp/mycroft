# -*- coding: utf-8 -*-
from mycroft.webapp import _create_application


def test_create_application():
    assert _create_application() is not None
