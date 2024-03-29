# ****************************************************************
# EMAIL UTILITIES
# ****************************************************************

import os
import re
import email
import imaplib
from email.header import decode_header
from exchangelib import Credentials, Account, Configuration, DELEGATE

HOTMAIL_USER = os.environ['HOTMAIL_USER']
HOTMAIL_PWD =  os.environ['HOTMAIL_PWD']

# ********************************************************************************
# Part 1: Sending Mail
# ********************************************************************************

def send_email(receiver_email, subject, content):
    port = 465  # For SSL
    smtp_server = "smtp.gmail.com"
    # sender_email = "babar.system@gmail.com"  
    sender_email = "raymond.delacaze@gmail.com"  
    password = os.environ['EMAIL_PWD']

    message = MIMEMultipart("alternative")
    message["Subject"] = subject
    message["From"] = sender_email
    message["To"] = receiver_email

    part1 = MIMEText(content, "plain")
    message.attach(part1)

    context = ssl.create_default_context()
    try:
        with smtplib.SMTP_SSL(smtp_server, port, context=context) as server:
            server.login(sender_email, password)
            server.sendmail(sender_email, receiver_email, message.as_string())
        return True
    except Exception as e:
        print("Unable to send mail: " + str(e))
        return False


# ********************************************************************************
# Part 2: Retrieving Mails
# ********************************************************************************

# Creating a script to access an email account requires careful handling
# as it involves sensitive information. It's recommended to have a good
# understanding of email protocols and security before proceeding.

# To access a Hotmail (now Outlook) account, you would typically use the
# IMAP protocol. Here’s a simplified example using the imaplib and email
# libraries in Python to fetch emails from a specific sender. Note that
# the actual code might need to be adjusted based on the exact
# requirements and email server settings:

def get_emails_from_sender(sender_email, user=HOTMAIL_USER, pwd=HOTMAIL_PWD):
    email = f'{user}@hotmail.com'
    
    # Set up the IMAP client
    imap = imaplib.IMAP4_SSL("outlook.office365.com")
    imap_port = 993

    # Login to the account
    imap.login(email, f'"{pwd}"')

    # Select the mailbox
    imap.select("inbox")

    # Search for emails from the specified sender
    status, messages = imap.search(None, f'FROM "{sender_email}"')
    if status != "OK":
        print(f"Error: {status}")
        return []

    # Fetch and print each email
    email_ids = messages[0].split()
    for email_id in email_ids:
        status, msg_data = imap.fetch(email_id, "(RFC822)")
        if status != "OK":
            print(f"Error: {status}")
            continue

        msg = email.message_from_bytes(msg_data[0][1])
        subject, encoding = decode_header(msg["subject"])[0]
        if isinstance(subject, bytes):
            subject = subject.decode(encoding if encoding else "utf-8")
        print(f"Subject: {subject}")

    # Logout and close the connection
    imap.logout()

# Usage:
# get_emails_from_sender("specific-sender@example.com")

# Replace "your-email@hotmail.com", "your-password", and
# "specific-sender@example.com" with your actual email address,
# password, and the sender's email address you are looking for,
# respectively.

# Please be aware:

# Storing your email password in plaintext within your script is a
# security risk. It's better to use OAuth2 or other more secure methods.
# This script only prints the subject of each email from the specified
# sender. You might need to modify the script to better suit your needs.
# Make sure to handle exceptions and errors in a production environment
# to ensure your script is robust and secure.  Moreover, consider
# looking into more feature-rich libraries such as exchangelib for
# interacting with Outlook/Hotmail, as they might provide a more robust
# and user-friendly interface for this task.


# ****************************************************************
# Microsoft Exchange Server
# ****************************************************************

SENDER = "noreply@medium.com"

def get_hotmail_messages(sender=SENDER, user=HOTMAIL_USER, pwd=HOTMAIL_PWD):
    
    email = f'{user}@hotmail.com'

    credentials = Credentials(email, pwd)

    config = Configuration(server="outlook.office365.com",
                           credentials=credentials)

    account = Account(primary_smtp_address="delaray@hotmail.com",
                      config=config,
                      autodiscover=False,
                      access_type=DELEGATE)

    folder = account.inbox
    
    items = folder.filter(sender=sender)
     
    messages = list(items)

    return messages


# -----------------------------------------------------------------
# Message URLS
# -----------------------------------------------------------------

def get_message_urls(message, prefix='https://medium.com'):

    # Define a regular expression pattern to match URLs
    url1 = r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]'
    url2 = r'|(?:%[0-9a-fA-F][0-9a-fA-F]))+'
    
    url_pattern = re.compile(url1+url2)
    
    # Extract URLs from the text and HTML bodies
    text_body_urls = url_pattern.findall(message.text_body or '')
    html_body_urls = url_pattern.findall(message.body or '')
    
    # Combine and deduplicate the lists of URLs
    urls = list(set(text_body_urls + html_body_urls))

    urls = [url for url in urls if url.startswith(prefix) is True]

    def shorten(url):
        try:
            pos = url.index('?source')
            return url[:pos]
        except Exception:
            return url

    urls = [shorten(url) for url in urls]
    
    return urls


# -----------------------------------------------------------------
# Message Properties
# -----------------------------------------------------------------

# Message object fileds and objects
# ['ELEMENT_NAME', 'FIELDS', 'ID_ELEMENT_CLS', 'INSERT_AFTER_FIELD', 'NAMESPACE', '___id', '__attachments', '__author', '__bcc_recipients',
# '__body', '__categories', '__cc_recipients', '__class__', '__conversation_id', '__conversation_index', '__conversation_topic',
# '__culture', '__datetime_created', '__datetime_received', '__datetime_sent', '__delattr__', '__dict__', '__dir__',
# '__display_cc', '__display_to', '__doc__', '__effective_rights', '__eq__', '__format__', '__ge__', '__getattribute__', '__gt__',
# '__has_attachments', '__hash__', '__headers', '__importance', '__in_reply_to', '__init__', '__init_subclass__', '__is_associated',
# '__is_delivery_receipt_requested', '__is_draft', '__is_from_me', '__is_read', '__is_read_receipt_requested', '__is_resend',
# '__is_response_requested', '__is_submitted', '__is_unmodified', '__item_class', '__last_modified_name', '__last_modified_time',
# '__le__', '__lt__', '__message_id', '__mime_content', '__module__', '__ne__', '__new__', '__parent_folder_id', '__received_by',
# '__received_representing', '__reduce__', '__reduce_ex__', '__references', '__reminder_due_by', '__reminder_is_set',
# '__reminder_message_data', '__reminder_minutes_before_start', '__reply_to', '__repr__', '__response_objects', '__sender',
# '__sensitivity', '__setattr__', '__size', '__sizeof__', '__slots__', '__str__', '__subclasshook__', '__subject', '__text_body',
# '__to_recipients', '__unique_body', '__web_client_edit_form_query_string',
# '__web_client_read_form_query_string', '_clear', '_create', '_delete', '_field_vals', '_fields_lock', '_id', '_slots_keys', '_update',
# '_update_fieldnames', 'account', 'add_field', 'archive', 'attach', 'attachments', 'attribute_fields', 'author', 'bcc_recipients', 'body',
# 'categories', 'cc_recipients', 'changekey', 'clean', 'conversation_id', 'conversation_index', 'conversation_topic', 'copy',
# 'create_forward', 'create_reply', 'create_reply_all', 'culture', 'datetime_created', 'datetime_received', 'datetime_sent', 'delete',
# 'deregister', 'detach', 'display_cc', 'display_to', 'effective_rights', 'folder', 'forward', 'from_xml',
# 'get_field_by_fieldname', 'has_attachments', 'headers', 'id', 'id_from_xml', 'importance', 'in_reply_to', 'is_associated',
# 'is_delivery_receipt_requested', 'is_draft', 'is_from_me', 'is_read', 'is_read_receipt_requested', 'is_resend', 'is_response_requested',
# 'is_submitted', 'is_unmodified', 'item_class', 'last_modified_name', 'last_modified_time', 'mark_as_junk', 'message_id', 'mime_content',
# 'move', 'move_to_trash', 'parent_folder_id', 'received_by', 'received_representing', 'references', 'refresh', 'register',
# 'reminder_due_by', 'reminder_is_set', 'reminder_message_data', 'reminder_minutes_before_start', 'remove_field', 'reply', 'reply_all',
# 'reply_to', 'request_tag', 'response_objects', 'response_tag', 'save', 'send', 'send_and_save', 'sender', 'sensitivity', 'size',
# 'soft_delete', 'subject', 'supported_fields', 'text_body', 'to_id', 'to_recipients', 'to_xml', 'unique_body', 'validate_field',
# 'web_client_edit_form_query_string', 'web_client_read_form_query_string']

def get_message_properties(message):

    # Collect essential message properties
    result = {'id': message.id,
              'sender': message.sender,
              'subject': message.subject,
              'text': message.text_body,
              'html': message.body,
              'recipients': message.to_recipients,
              'size': message.size,
              'datetime_sent': mmessage.datetime_sent,
              'datetime_received': message.datetime_received}

    return result
    
    
# ****************************************************************
# End of File
# ****************************************************************

