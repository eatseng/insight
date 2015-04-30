from __future__ import absolute_import

from clay import config, mail


def error_email(**kwargs):
    if kwargs.get('table_name', None) is None or kwargs.get('error', None) is None:
        return

    if config.get('debug.enabled') is False:
        subj_str = "Job Failure - %s" % kwargs.get('table_name')
        mail_str = "Job %s failed with error: %s" % (kwargs.get('table_name'), kwargs.get('error'))
        mail_str = mail_str + ' || Stack trace: %s' % kwargs.get('trace', None)
        mail.sendmail("etseng@fuzebox.com", subj_str, mail_str)
