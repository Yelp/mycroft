# -*- coding: utf-8 -*-

from __future__ import absolute_import

import socket
import smtplib
from email.MIMEText import MIMEText

import staticconf


class Mailer:
    def __init__(self, run_local):
        self.subject = 'mycroft job {0} status changed'
        host = socket.gethostname()
        self.link_temp = 'http://' + \
            host + ':11476/web/#/jobs/view?uuid={0}'
        self.template = """Job {0}
status: {1}
schema name: {2}
schema version: {3}
s3 path: {4}
redshift id: {5}
start date: {6}
end date: {7}
link: {8}
additional info: {9}"""
        self.address = "mycroft@{0}".format(host)

    def mail_result(self, final_status, msg, additional_info=None):
        link = self.link_temp.format(msg['uuid'])
        content = self.template.format(
            msg['uuid'], final_status, msg['log_name'], msg['log_schema_version'],
            msg['s3_path'], msg['redshift_id'], msg['start_date'], msg['end_date'], link,
            additional_info
        )

        new_msg = MIMEText(content)
        new_msg['Subject'] = self.subject.format(msg['uuid'])
        new_msg['From'] = self.address
        new_msg['To'] = ','.join(msg['contact_emails'])

        smtp_host = staticconf.read_string('smtp_host', 'localhost')
        smtp_port = staticconf.read_string('smtp_port', None)
        smtp_login = staticconf.read_string('smtp_login', None)
        smtp_password = staticconf.read_string('smtp_password', None)
        smtp_security = staticconf.read_string('smtp_security', None)

        if smtp_port is not None:
            smtp_host = "{0}:{1}".format(smtp_host, smtp_port)

        if smtp_security is not None:
            smtp_security = smtp_security.upper()

        if smtp_security == 'SSL':
            s = smtplib.SMTP_SSL(smtp_host)
            s.login(smtp_login, smtp_password)
        elif smtp_security == 'TLS':
            s = smtplib.SMTP(smtp_host)
            s.ehlo()
            s.starttls()
            s.login(smtp_login, smtp_password)
        else:
            s = smtplib.SMTP(smtp_host)

        s.sendmail(self.address, msg['contact_emails'], new_msg.as_string())
        s.quit()
