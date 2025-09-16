#!/usr/bin/env python3
import os, smtplib, mimetypes, glob
from email.message import EmailMessage

def attach(pattern, msg):
    for path in glob.glob(pattern):
        ctype, encoding = mimetypes.guess_type(path)
        if ctype is None or encoding is not None:
            ctype = 'application/octet-stream'
        maintype, subtype = ctype.split('/', 1)
        with open(path, 'rb') as f:
            msg.add_attachment(f.read(), maintype=maintype, subtype=subtype, filename=os.path.basename(path))

smtp_host = os.environ.get('SMTP_HOST')
if not smtp_host:
    print('SMTP_HOST not set, skip email')
    raise SystemExit(0)
msg = EmailMessage()
msg['From'] = os.environ.get('MAIL_FROM','noreply@example.com')
msg['To'] = os.environ.get('MAIL_TO','root@example.com')
msg['Subject'] = 'E2E Observability Report'
with open('report.md','r',encoding='utf-8') as f:
    msg.set_content(f.read())

attach('dashboard_*.json', msg)
attach('panel_*.png', msg)

smtp_port = int(os.environ.get('SMTP_PORT','587'))
user = os.environ.get('SMTP_USER')
password = os.environ.get('SMTP_PASS')

with smtplib.SMTP(smtp_host, smtp_port) as s:
    s.starttls()
    if user and password:
        s.login(user, password)
    s.send_message(msg)
print('Email sent (if SMTP settings were correct).')
