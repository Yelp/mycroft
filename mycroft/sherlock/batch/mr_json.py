# -*- coding: utf-8 -*-
"""

How to test run in dev:

make emr

zcat tests/example_user_session_metrics.gz | inenv -- \
        python sherlock/batch/mr_json.py --runner=local \
        --extractions schema/user_session_metrics.yaml

"""
import simplejson

from sherlock.batch.mr_redshift_etl import derive_filename
from sherlock.batch.mr_redshift_etl import MRRedshiftETL


class MRJsonETL(MRRedshiftETL):

    METRIC_NAME_TO_EVAL_FUNC = {
        '__source_filename__': derive_filename,
    }

    def derive_metric(self,
                      name_to_object=None, metric_name=None, context=None):
        return self.METRIC_NAME_TO_EVAL_FUNC[metric_name](name_to_object,
                                                          context=context)

    def mapper(self, _, value):
        """
        This takes a single json line and loads a python dict based.
        It then emits the key, value based on the YAML schema
        """
        try:
            single_entry_dict = simplejson.loads(value)
            for k, v in self.emit_rows_for_tables(single_entry_dict):
                yield k, v
        except simplejson.JSONDecodeError:
            yield self.emit_exception_row(value)


def mrjob_create(args=None):
    return MRJsonETL(args=args)


if __name__ == '__main__':
    MRJsonETL.run()
