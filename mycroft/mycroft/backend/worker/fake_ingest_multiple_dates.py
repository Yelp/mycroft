# -*- coding: utf-8 -*-
import time
from random import random
from datetime import timedelta
from multiprocessing import ProcessError
from staticconf import read_string
from sherlock.common.pipeline import load_io_yaml_from_args
from sherlock.batch.ingest_multiple_dates import KeyboardInterruptError


def ingest_multiple_dates_main(args_namespace):
    """ Main entry point for this script. Will parse args passed in and run
    ET/L as appropriate.
    Does not throw any exception.
    """
    results = []
    start_time = time.time()
    try:
        load_io_yaml_from_args(args_namespace.io_yaml)
        d = args_namespace.start_date
        end_date = args_namespace.end_date
        delta = timedelta(days=1)
        try:
            check_status()
            while d <= end_date:
                start_time = time.time()
                generate_random_error(0)
                time.sleep(100)
                end_time = time.time()
                results.append(
                    {
                        'date': d.strftime("%Y-%m-%d"),
                        'status': 'success',
                        'start_time': start_time,
                        'end_time': end_time,
                        'error_info': {}
                    }
                )
                d += delta
        except ProcessError as pe:
            results += pe.args[0]['results']
            pass
    except (KeyboardInterrupt, KeyboardInterruptError, BaseException) as e:
        if type(e) in [KeyboardInterrupt, KeyboardInterruptError]:
            status = 'cancelled'
        else:
            status = 'error'
        results.append(
            {
                'date': args_namespace.start_date.strftime("%Y-%m-%d"),
                'status': status,
                'start_time': start_time,
                'end_time': time.time(),
                'error_info': get_error_info(status)
            }
        )

    return results


def check_status():
    s3_path = read_string('pipeline.et_step.s3_prefixes', default='')
    if 'fake_wrong' in s3_path:
        raise BaseException('Fake Exception')


def generate_random_error(error_rate):
    if random() < error_rate:
        raise BaseException('Fake Exception')


def get_error_info(status):
    if status == 'error':
        return {
            'crash_tb': 'fake error',
            'crash_exc': 'fake error',
        }
    return {}
