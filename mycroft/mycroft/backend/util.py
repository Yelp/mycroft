# -*- coding: utf-8 -*-
from datetime import timedelta, datetime

from mycroft.models.scheduled_jobs import JOBS_ETL_STATUS_ERROR
from mycroft.models.scheduled_jobs import JOBS_ETL_STATUS_SUCCESS
from mycroft.models.scheduled_jobs import JOBS_ETL_STATUS_COMPLETE


def date_string_to_datetime(date):
    return datetime.strptime(date, "%Y-%m-%d")


def datetime_to_date_string(dt):
    return dt.strftime("%Y-%m-%d")


def next_date_for_string(date, step=1):
    """ Get the YYYY-MM-DD format string for step days plus date
    :param date: a string representation of a particular date
    :type date: string or None
    :param step: how many days from date is next date
    :type step: int

    :returns: YYYY-MM-DD formatted date
    :rtype: string
    """
    if date is None:
        return None
    day = date_string_to_datetime(date)
    return datetime_to_date_string(day + timedelta(days=step))


def date_string_total_items(start, end, step=1):
    """ Returns # of runs in a date string range
    :param start: a string representation of a start date
    :type start: string
    :param end: a string representation of an end date
    :type end: string
    :param step: an integer step function to use
    :returns: # of runs in a date string range
    :rtype: int
    """
    cur_date = date_string_to_datetime(start)
    end_date = date_string_to_datetime(end)
    return len(xrange(0, (end_date - cur_date).days + 1, step))


def date_string_generator(start, end, step=1):
    """ A generator of YYYY-MM-DD format strings
    :param start: a string representation of a start date
    :type start: string
    :param end: a string representation of an end date
    :type end: string
    :param step: an integer step function to use
    :returns: YYYY-MM-DD formatted date
    :rtype: string
    """
    cur_date = date_string_to_datetime(start)
    end_date = date_string_to_datetime(end)
    while cur_date <= end_date:
        yield datetime_to_date_string(cur_date)
        cur_date += timedelta(days=step)


def parse_results(results, end_date=None):
    """ Parse the results list from worker job
    and return a status, date tuple.

    :param results: list of status results from sherlock
    :type results: a dictionary of results(list of steps) keyed by label, eg: 2014-10-12

    :returns: 3 tuple containing status, date of last successful run, any extra info
    :rtype: list
    """
    status = JOBS_ETL_STATUS_SUCCESS
    lsr = None  # lsl = last successful run for this work item

    if not results:
        return JOBS_ETL_STATUS_ERROR, None, None

    for run_label in sorted(results,
                            key=lambda x: date_string_to_datetime(x)):
        for record in results[run_label]:
            if record['status'] == 'error' or record['status'] == 'unknown':
                return JOBS_ETL_STATUS_ERROR, lsr, record.get('error_info')
            elif record['status'] != 'success':
                return status, lsr, None
        lsr = run_label

    if end_date is not None and lsr == end_date:
        status = JOBS_ETL_STATUS_COMPLETE

    return status, lsr, None
