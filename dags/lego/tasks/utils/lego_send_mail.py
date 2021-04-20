from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from email.utils import formatdate
from email.mime.text import MIMEText
import smtplib
import os
import imp

env = os.getenv('airflow_env').lower()


if env in ('dev', 'qa'):
    SMTP_USER = "airflow-dev@mail.lego.cn"
    smtp_mail_from = "airflow-dev@mail.lego.cn"
    kms_smtp_conf="dev_smtp_pwd"
else:
    SMTP_USER = "airflow@mail.lego.cn"
    smtp_mail_from = "airflow@mail.lego.cn"
    kms_smtp_conf="prd_smtp_pwd"
kms_client_path = "/cdp/work_dir/gp_conn_cmd.py"

SMTP_HOST = "smtpdm.aliyun.com"
SMTP_PORT = 80
SMTP_STARTTLS = True
SMTP_SSL = False
gp_conn_cmd = imp.load_source('gp_conn_cmd',kms_client_path )

def send_email(to, subject, html_content,
               files=None, dryrun=False, cc=None, bcc=None,
               mime_subtype='mixed', mime_charset='utf-8', **kwargs):
    """
    Send email using backend specified in EMAIL_BACKEND.
    """
    
    to = get_email_address_list(to)
    to = ", ".join(to)

    return send_email_smtp(to, subject, html_content, files=files,
                   dryrun=dryrun, cc=cc, bcc=bcc,
                   mime_subtype=mime_subtype, mime_charset=mime_charset, **kwargs)


def send_email_smtp(to, subject, html_content, files=None,
                    dryrun=False, cc=None, bcc=None,
                    mime_subtype='mixed', mime_charset='utf-8',
                    **kwargs):
    """
    Send an email with html content

    >>> send_email('test@example.com', 'foo', '<b>Foo</b> bar', ['/dev/null'], dryrun=True)
    """
    to = get_email_address_list(to)

    msg = MIMEMultipart(mime_subtype)
    msg['Subject'] = subject
    msg['From'] = smtp_mail_from
    msg['To'] = ", ".join(to)
    recipients = to
    if cc:
        cc = get_email_address_list(cc)
        msg['CC'] = ", ".join(cc)
        recipients = recipients + cc

    if bcc:
        # don't add bcc in header
        bcc = get_email_address_list(bcc)
        recipients = recipients + bcc

    msg['Date'] = formatdate(localtime=True)
    mime_text = MIMEText(html_content, 'html', mime_charset)
    msg.attach(mime_text)

    for fname in files or []:
        basename = os.path.basename(fname)
        with open(fname, "rb") as f:
            part = MIMEApplication(
                f.read(),
                Name=basename
            )
            part['Content-Disposition'] = 'attachment; filename="%s"' % basename
            part['Content-ID'] = '<%s>' % basename
            msg.attach(part)

    send_MIME_email(smtp_mail_from, recipients, msg, dryrun)


def send_MIME_email(e_from, e_to, mime_msg, dryrun=False):
    SMTP_PASSWORD = gp_conn_cmd.get_pwd(kms_smtp_conf)
    if not dryrun:
        s = smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT) if SMTP_SSL else smtplib.SMTP(SMTP_HOST, SMTP_PORT)
        if SMTP_STARTTLS:
            s.starttls()
        if SMTP_USER and SMTP_PASSWORD:
            s.login(SMTP_USER, SMTP_PASSWORD)
        s.sendmail(e_from, e_to, mime_msg.as_string())
        s.quit()

def get_email_address_list(address_string):
    if ',' in address_string:
        address_string = [address.strip() for address in address_string.split(',')]
    elif ';' in address_string:
        address_string = [address.strip() for address in address_string.split(';')]
    else:
        address_string = [address_string]

    return address_string


import argparse
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Send out mail")
    parser.add_argument('-t', "--to") 
    parser.add_argument('-s', "--subject") 
    parser.add_argument('-c', "--content")
    parser.add_argument('-f', "--files")
    args = parser.parse_args()
    to = args.to
    subject = args.subject
    content = args.content 
    files = ""
    if args.files is not None:
        files = args.files.split(",")

    send_email(to, subject, content, files)
