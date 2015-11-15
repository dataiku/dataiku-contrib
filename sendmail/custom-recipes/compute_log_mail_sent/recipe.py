# Code for custom code recipe compute_log_mail_sent (imported from a Python recipe)

# To finish creating your custom recipe from your original Python recipe, you'll need to do
# the following:
#  - Declare the input and output roles in the recipe.json file
#  - Replace the dataset names by roles access in your code
#  - Declare, if any, the params of your custom recipe in the recipe.json file
#  - Replace the hardcoded params values by acccess to the configuration map

# Sample code for all of this is included before your own code
# Please also see the "recipe.json" file for more information

# import the classes for accessing DSS objects from the recipe
import dataiku
# Import the helpers for custom recipes
from dataiku.customrecipe import *

# Inputs and outputs are defined by roles. In the recipe's I/O tab, the user can associate one
# or more dataset to each input and output role, depending on the role characteristics.
# Roles need to be defined in the recipe.json, in the inputRoles and outputRoles fields.

# To  retrieve the datasets of an input role named 'input_A' as an array of dataset names:
people = dataiku.Dataset(get_input_names_for_role('input')[0])

# For outputs, the process is the same:
output_A_names = get_output_names_for_role('output')
output = dataiku.Dataset(output_A_names[0]) if len(output_A_names) > 0 else None

attachments = [dataiku.Dataset(x) for x in get_input_names_for_role('attachment')]

# The configuration consists of the parameters set up by the user in the recipe Settings tab.

# Parameters must be added to the recipe.json file so that DSS can prompt the user for values in
# the Settings tab of the recipe. The field "params" holds a list of all the params for wich the
# user will be prompted for values.

# The configuration is simply a map of parameters, and retrieving the value of one of them is simply:
config = get_recipe_config()
recipient_column = config['recipient_column'] if 'recipient_column' in config else None
recipient_value = config['recipient_value'] if 'recipient_value' in config else None
sender_column = config['sender_column'] if 'sender_column' in config else None
sender_value = config['sender_value'] if 'sender_value' in config else None
subject_column = config['subject_column'] if 'subject_column' in config else None
subject_value = config['subject_value'] if 'subject_value' in config else None
body_column = config['body_column'] if 'body_column' in config else None
body_value = config['body_value'] if 'body_value' in config else None
smtp_host = config['smtp_host'] if 'smtp_host' in config else None
smtp_port = int(config['smtp_port']) if 'smtp_port' in config else None

attachment_type = config['attachment_type'] if 'attachment_type' in config else "csv"


print smtp_host
print "Port:", smtp_port, attachment_type

#############################
# Your original recipe
#############################

# -*- coding: utf-8 -*-
import dataiku
import pandas as pd, numpy as np
from dataiku import pandasutils as pdu

# Input datasets
# Import smtplib for the actual sending function
import smtplib

# Import the email modules we'll need
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication

schema = list(people.read_schema())
schema.append({'name':'STATUS', 'type':'STRING'})
schema.append({'name':'mailsend_error', 'type':'STRING'})
if output: 
    output.write_schema(schema)
    writer = output.get_writer()
else:
    writer = None
        
 
    msg.attach(MIMEText(text))

    for f in files or []:
        with open(f, "rb") as fil:
            msg.attach(MIMEApplication(
                fil.read(),
                Content_Disposition='attachment; filename="%s"' % basename(f),
                Name=basename(f)
            ))


mime_apps = []

import StringIO

for a in attachments:
    dataframe = a.get_dataframe()
    io = StringIO.StringIO()

    if attachment_type == 'excel': 
        # Use a temp filename to keep pandas happy.
        excel = pd.ExcelWriter('temp.xlsx', engine="xlsxwriter")
        # Set the filename/file handle in the xlsxwriter.workbook object.
        excel.book.filename = io
        # Write the data frame to the StringIO object.
        dataframe.to_excel(excel, sheet_name='Sheet1', index=False)
        excel.save()
        xlsx_data = io.getvalue()
        datasetname = a.full_name + ".xlsx"
        app = MIMEApplication(xlsx_data)
        app.add_header("Content-Disposition", 'attachment', filename=datasetname)
        mime_apps.append(app)
    else:
        dataframe.to_csv(io)
        datasetname = a.full_name + ".csv"
        app = MIMEApplication(io.getvalue(), _subtype="csv",
            Content_Disposition='attachment; filename=%s.csv' % datasetname, 
            Name=datasetname)
        mime_apps.append(app)


def send_email(contact): 
    if recipient_value:
        recipient = recipient_value
    elif recipient_column: 
        recipient = contact[recipient_column]
    if body_value:
        email_text = body_value
    elif body_column:
        email_text = contact[body_column]
    if subject_value:
        email_subject = subject_value
    elif subject_column:
        email_subject = contact[subject_column]
    if sender_value:
        sender = sender_value
    elif sender_column:
        sender = sender_column
    msg = MIMEMultipart()
    msg["From"] = sender
    msg["To"] =recipient
    msg["Subject"]= email_subject
    msg.attach(MIMEText(email_text))
    for a in mime_apps:
        msg.attach(a)
    s = smtplib.SMTP(smtp_host, port=smtp_port)
    s.sendmail(sender, [recipient], msg.as_string())
    s.quit()
    
for contact in people.iter_rows():
    try:
        send_email(contact)
        d = dict(contact)
        d['STATUS'] = 'SUCCESS'
        if writer:
            writer.write_row_dict(d)
    except Exception as e:
        d = dict(contact)
        d['STATUS'] = 'FAILED' 
        d['mailsend_error'] = str(e)
        if writer:
            writer.write_row_dict(d)       

