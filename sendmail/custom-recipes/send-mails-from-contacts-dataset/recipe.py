import dataiku
from dataiku.customrecipe import *
import pandas as pd
import logging
# Import smtplib for the actual sending function
import smtplib
# Import the email modules we'll need
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
import StringIO

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

# Get handles on datasets
output_A_names = get_output_names_for_role('output')
output = dataiku.Dataset(output_A_names[0]) if len(output_A_names) > 0 else None

people = dataiku.Dataset(get_input_names_for_role('contacts')[0])
attachments = [dataiku.Dataset(x) for x in get_input_names_for_role('attachments')]

# Read configuration
config = get_recipe_config()

recipient_column = config.get('recipient_column', None)

sender_column = config.get('sender_column', None)
sender_value = config.get('sender_value', None)
use_sender_value = config.get('use_sender_value', False)

subject_column = config.get('subject_column', None)
subject_value = config.get('subject_value', None)
use_subject_value = config.get('use_subject_value', False)

body_column = config.get('body_column', None)
body_value = config.get('body_value', None)
body_encoding = config.get('body_encoding', 'us-ascii')
use_body_value = config.get('use_body_value', False)

smtp_host = config.get('smtp_host', None)
smtp_port = int(config.get('smtp_port', "25"))

attachment_type = config.get('attachment_type', "csv")

output_schema = list(people.read_schema())
output_schema.append({'name':'sendmail_status', 'type':'string'})
output_schema.append({'name':'sendmail_error', 'type':'string'})
output.write_schema(output_schema)

if not body_column and not body_value:
    raise AttributeError("No body column nor body value specified")

people_columns = [ p['name'] for p in people.read_schema() ]
for arg in ['sender', 'subject', 'body']:
    if not globals()["use_" + arg + "_value"] and globals()[arg + "_column"] not in people_columns:
        raise AttributeError("The column you specified for %s (%s) was not found." % (arg, globals()[arg + "_column"]))

# Prepare attachements
mime_parts = []

for a in attachments:

    if attachment_type == "excel":
        request_fmt = "excel"
    else:
        request_fmt = "tsv-excel-header"
        filename = a.full_name + ".csv"
        mimetype = ["text", "csv"]

    with a.raw_formatted_data(format=request_fmt) as stream:
        buf = stream.read()

    if attachment_type == "excel":
        app = MIMEApplication(buf, _subtype="vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        app.add_header("Content-Disposition", 'attachment', filename= a.full_name + ".xlsx")
        mime_parts.append(app)
    else:
        txt = MIMEText(buf, _subtype="csv", _charset="utf-8")
        txt.add_header("Content-Disposition", 'attachment', filename= a.full_name + ".csv")
        mime_parts.append(txt)


s = smtplib.SMTP(smtp_host, port=smtp_port)


def send_email(contact):
    recipient = contact[recipient_column]
    email_text = body_value if use_body_value else contact.get(body_column, "")
    email_subject = subject_value if use_subject_value else contact.get(subject_column, "")
    sender = sender_value if use_sender_value else contact.get(sender_column, "")
    
    msg = MIMEMultipart()

    msg["From"] = sender
    msg["To"] = recipient
    msg["Subject"]=  email_subject

    # Leave some space for proper displaying of the attachment
    msg.attach(MIMEText(email_text + '\n\n', 'plain', body_encoding))
    for a in mime_parts:
        msg.attach(a)

    s.sendmail(sender, [recipient], msg.as_string())
    

with output.get_writer() as writer:
    i = 0
    success=0
    fail = 0
    for contact in people.iter_rows():
        logging.info("Sending to %s" % contact)
        try:
            send_email(contact)
            d = dict(contact)
            d['sendmail_status'] = 'SUCCESS'
            success+=1
            if writer:
                writer.write_row_dict(d)
        except Exception as e:
            logging.exception("Send failed")
            fail+=1
            d = dict(contact)
            d['sendmail_status'] = 'FAILED'
            d['sendmail_error'] = str(e)
            if writer:
                writer.write_row_dict(d)

        i +=1
        if i % 5 == 0:
            logging.info("Sent %d mails (%d success %d fail)" % (i, success, fail))

s.quit()