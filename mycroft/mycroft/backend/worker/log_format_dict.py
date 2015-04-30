# -*- coding: utf-8 -*-

FORMAT_TO_MRJOB = {
    'json': 'sherlock.batch.mr_json.mrjob_create',
    'search_session': 'sherlock.batch.mr_search_session_etl.mrjob_create',
    'suggest_session': 'sherlock.batch.mr_suggest_session_etl.mrjob_create',
    'mysql': 'sherlock.batch.mr_mysqldump.mrjob_create',
    'access': 'sherlock.batch.mr_access_log.mrjob_create',
    'write_review': 'sherlock.batch.mr_war.mrjob_create',
    'custom': 'sherlock.batch.{0}.mrjob_create',
}
