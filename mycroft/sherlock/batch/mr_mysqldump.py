# -*- coding: utf-8 -*-
"""

How to test run in dev:

make emr

cat tests/example_business_data.sql | inenv -- \
        python sherlock/batch/mr_mysqldump.py --runner=local \
        --extractions schema/business.yaml

"""
from mr3po.mysqldump import parse_insert
from sherlock.batch.mr_redshift_etl import MRRedshiftETL


class MysqldumpETL(MRRedshiftETL):

    def mapper(self, _, sqldump_row):
        """Takes SQL 'INSERT INTO...' statement, converts it into a dictionary,
        then proceeds to yield PSV format
        """
        table, row = parse_insert(sqldump_row, single_row=True, complete=True)
        for k, v in self.emit_rows_for_tables(row):
            yield k, v


def mrjob_create(args=None):
    return MysqldumpETL(args=args)


if __name__ == '__main__':
    MysqldumpETL.run()
