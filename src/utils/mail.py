# ****************************************************************
# EMAIL UTILITIES
# ****************************************************************

# File contents:

# 1: Microsoft Graph API
# 2: Sending Gmail Messages
# 3: Message Properties and Content

# ****************************************************************

import os
import requests
import msal
import ssl
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from dotenv import load_dotenv

load_dotenv()

# ****************************************************************
# Part 1: Microsoft Graph API
# ****************************************************************

# Graph Reader APP:

# https://portal.azure.com/#view/Microsoft_AAD_RegisteredApps/
# ApplicationMenuBlade/~/Overview/appId/939a2318-2320-4626-94de-f637351b720e/
# isMSAApp~/false

# Redirect URI: https://login.microsoftonline.com/common/oauth2/nativeclient

HOTMAIL_USER = os.environ['HOTMAIL_USER']
HOTMAIL_PWD = os.environ['HOTMAIL_PWD']
HOTMAIL_SERVER = os.environ.get('HOTMAIL_SERVER')


# -----------------------------------------------------------------
# MS Graph API Authentication (Using device code flow)
# -----------------------------------------------------------------

# From app registration
GRAPH_READER_CLIENT_ID = os.getenv('GRAPH_READER_CLIENT_ID')

# Use "consumers" for personal Hotmail/Outlook.com accounts
GRAPH_READER_TENANT = os.getenv('GRAPH_READER_TENANT', 'consumers')

# Authority URL  and scopes
GRAPH_READER_AUTHORITY = os.getenv('GRAPH_READER_AUTHORITY')
GRAPH_READER_SCOPES = os.getenv('GRAPH_READER_SCOPES', '').split(',')


# -----------------------------------------------------------------
# Get MS Graph API Token (Interactive)
# -----------------------------------------------------------------

def get_token_interactive(client_id=GRAPH_READER_CLIENT_ID,
                          authority=GRAPH_READER_AUTHORITY):
    """
    Get an access token using device code flow for Microsoft Graph API.
    """

    app = msal.PublicClientApplication(client_id, authority=authority)

    # Acquire token using device code flow
    flow = app.initiate_device_flow(scopes=GRAPH_READER_SCOPES)
    if "user_code" not in flow:
        raise Exception("Failed to create device flow. " +
                        "Check app registration.")

    # Instructs you to visit a URL and enter a code
    print(flow["message"])
    result = app.acquire_token_by_device_flow(flow)

    if "access_token" in result:
        return result['access_token']

    else:
        raise Exception(f"Failed to acquire token: {result}")


# -----------------------------------------------------------------
# MS Graph API Authentication (Using token cache)
# -----------------------------------------------------------------

DATA_DIR = os.getenv('DATA_DIR')
GRAPH_READER_CACHE_FILE = f"{DATA_DIR}/msal_cache.json"


# -----------------------------------------------------------------

def load_cache():
    cache = msal.SerializableTokenCache()
    if os.path.exists(GRAPH_READER_CACHE_FILE):
        with open(GRAPH_READER_CACHE_FILE, "r") as f:
            cache.deserialize(f.read())
    return cache


def save_cache(cache, cache_file=GRAPH_READER_CACHE_FILE):
    if cache.has_state_changed:
        with open(cache_file, "w") as f:
            f.write(cache.serialize())


def get_token_silent(client_id=GRAPH_READER_CLIENT_ID,
                     authority=GRAPH_READER_AUTHORITY,
                     scopes=GRAPH_READER_SCOPES):
    """
    Get an access token using silent authentication with token cache.
    """
    cache = load_cache()

    app = msal.PublicClientApplication(client_id,
                                       authority=authority,
                                       token_cache=cache)

    # Try silent first
    result = app.acquire_token_silent(scopes, account=None)

    if not result:
        # Fallback to device code flow
        flow = app.initiate_device_flow(scopes=GRAPH_READER_SCOPES)
        if "user_code" not in flow:
            raise Exception(f"Device flow failed: {flow}")
        print(flow["message"])
        result = app.acquire_token_by_device_flow(flow)

    if "access_token" not in result:
        raise Exception(f"Token acquisition failed: {result}")

    save_cache(cache)

    return result["access_token"]


# -----------------------------------------------------------------
# Get MS Hotmail Messages
# -----------------------------------------------------------------

def get_hotmail_messages(access_token: str, top: int = 25
                         ) -> list:

    url = "https://graph.microsoft.com/v1.0/me/messages"

    headers = {"Authorization": f"Bearer {access_token}"}
    params = {"$top": top, "$select": "subject,from, " +
              "receivedDateTime, sentDateTime, bodyContent",
              "$orderby": "receivedDateTime desc"
    }

    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()

    return response.json().get("value", [])


# -----------------------------------------------------------------

def get_hotmail_messages_from_sender(sender_email: str, access_token: str,
                                     top: int | None = None) -> list:
    """
    Retrieve recent messages from a specific sender.
    """
    url = "https://graph.microsoft.com/v1.0/me/messages"
    headers = {"Authorization": f"Bearer {access_token}"}
    params = {"$top": top, "$select": "subject,from, " +
              "receivedDateTime, sentDateTime, bodyContent",
              "$orderby": "receivedDateTime desc"
              }
    if top:
        params["$top"] = str(top)

    resp = requests.get(url, headers=headers, params=params)
    resp.raise_for_status()

    return resp.json().get("value", [])


# -----------------------------------------------------------------
# Get MS Hotmail Message Content
# -----------------------------------------------------------------

def get_hotmail_message_content(access_token, message_id):
    """
    Retrieve the subject, sender, and full body content of a message by ID.
    """
    url = f"https://graph.microsoft.com/v1.0/me/messages/{message_id}"
    headers = {"Authorization": f"Bearer {access_token}"}
    params = {"$select": "subject,from,body,receivedDateTime"}
    resp = requests.get(url, headers=headers, params=params)
    resp.raise_for_status()
    msg = resp.json()

    return {
        "id": msg["id"],
        "subject": msg["subject"],
        "from": msg["from"]["emailAddress"]["address"],
        "date": msg["receivedDateTime"],
        # can be HTML or text depending on `contentType`
        "body": msg["body"]["content"]
    }


# ------------------------------------------------------------------
# List Hotmail Folders
# ------------------------------------------------------------------

def list_hotmail_folders(access_token: str) -> list:
    """List all mail folders in the user's mailbox.

    Args:
        access_token (str): A valid access token for the Microsoft Graph API.

    Returns:
        list: A list of mail folders.
    """

    url = "https://graph.microsoft.com/v1.0/me/mailFolders"
    headers = {"Authorization": f"Bearer {access_token}"}
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()

    return resp.json()["value"]


# ------------------------------------------------------------------
# Move Hotmail Message to another folder
# ------------------------------------------------------------------

def move_hotmail_message(access_token: str, message_id: str,
                         destination_folder_id: str) -> dict:
    """_summary_

    Args:
        access_token (str): _A valid access token for the Microsoft Graph API_
        message_id (str): _The ID of the message to move_
        destination_folder_id (str): _The ID of the destination folder_

    Returns:
        _type_: _description_
    """

    url = f"https://graph.microsoft.com/v1.0/me/messages/{message_id}/move"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }
    data = {"destinationId": destination_folder_id}
    resp = requests.post(url, headers=headers, json=data)
    resp.raise_for_status()

    return resp.json()


# -----------------------------------------------------------------
# Decode JSON Web Token (JWT)
# -----------------------------------------------------------------

# MS Graph API access tokens are not JWTs, but some systems
# use JWTs for auth.

# def decode_jwt(token):
#     # Split token into 3 parts
#     parts = token.split(".")
#     if len(parts) < 2:
#         raise ValueError("Not a valid JWT")

#     # Decode payload (2nd part)
#     payload = parts[1] + "=="  # pad
#     decoded = base64.urlsafe_b64decode(payload)
#     data = json.loads(decoded)
#     return data


# ********************************************************************************
# Part 2: Sending Gmail Messages
# ********************************************************************************

GMAIL_EMAIL = os.environ.get('GMAIL_EMAIL')
GMAIL_PWD = os.environ.get('GMAIL_PWD')

GMAIL_SMTP_SERVER = os.environ.get('GMAIL_SMTP_SERVER', 'smtp.gmail.com')
GMAIL_SMTP_PORT = os.environ.get('GMAIL_SMTP_PORT', 465)


# -----------------------------------------------------------------

def send_email(receiver_email: str,
               subject: str,
               content: str,
               sender_email: str,
               sender_pwd: str):

    # For SSL
    port = GMAIL_SMTP_PORT
    port = int(port) if port else 465
    smtp_server = GMAIL_SMTP_SERVER

    message = MIMEMultipart("alternative")
    message["Subject"] = subject
    message["From"] = sender_email
    message["To"] = receiver_email

    part1 = MIMEText(content, "plain")
    message.attach(part1)

    context = ssl.create_default_context()

    try:
        with smtplib.SMTP_SSL(smtp_server, port, context=context) as server:
            server.login(sender_email, sender_pwd)
            server.sendmail(sender_email, receiver_email, message.as_string())
        return True

    except Exception as e:
        print("Unable to send mail: " + str(e))
        return False


# ****************************************************************
# Part 3: Message Properties and Content
# ****************************************************************

# -----------------------------------------------------------------
# Message Properties
# -----------------------------------------------------------------

# MS Exchange Message object all fields and methods:

# ['ELEMENT_NAME', 'FIELDS', 'ID_ELEMENT_CLS', 'INSERT_AFTER_FIELD',
# 'NAMESPACE', '___id', '__attachments', '__author', '__bcc_recipients',
# '__body', '__categories', '__cc_recipients', '__class__',
# '__conversation_id', '__conversation_index', '__conversation_topic',
# '__culture', '__datetime_created', '__datetime_received', '__datetime_sent',
# '__delattr__', '__dict__', '__dir__',
# '__display_cc', '__display_to', '__doc__', '__effective_rights', '__eq__',
# '__format__', '__ge__', '__getattribute__', '__gt__',
# '__has_attachments', '__hash__', '__headers', '__importance',
# '__in_reply_to', '__init__', '__init_subclass__', '__is_associated',
# '__is_delivery_receipt_requested', '__is_draft', '__is_from_me', '__is_read',
# '__is_read_receipt_requested', '__is_resend',
# '__is_response_requested', '__is_submitted', '__is_unmodified',
# '__item_class',
# '__last_modified_name', '__last_modified_time',
# '__le__', '__lt__', '__message_id', '__mime_content', '__module__',
# '__ne__', '__new__', '__parent_folder_id', '__received_by',
# '__received_representing', '__reduce__', '__reduce_ex__', '__references',
# '__reminder_due_by', '__reminder_is_set',
# '__reminder_message_data', '__reminder_minutes_before_start', '__reply_to',
# '__repr__', '__response_objects', '__sender',
# '__sensitivity', '__setattr__', '__size', '__sizeof__', '__slots__',
# '__str_ _', '__subclasshook__', '__subject', '__text_body',
# '__to_recipients', '__unique_body', '__web_client_edit_form_query_string',
# '__web_client_read_form_query_string', '_clear', '_create', '_delete',
# '_field_vals', '_fields_lock', '_id', '_slots_keys', '_update',
# '_update_fieldnames', 'account', 'add_field', 'archive', 'attach',
# 'attachments', 'attribute_fields', 'author', 'bcc_recipients', 'body',
# 'categories', 'cc_recipients', 'changekey', 'clean', 'conversation_id',
# 'conversation_index', 'conversation_topic', 'copy',
# 'create_forward', 'create_reply', 'create_reply_all', 'culture',
# 'datetime_created', 'datetime_received', 'datetime_sent', 'delete',
# 'deregister', 'detach', 'display_cc', 'display_to', 'effective_rights',
# 'folder', 'forward', 'from_xml',
# 'get_field_by_fieldname', 'has_attachments', 'headers', 'id', 'id_from_xml',
# 'importance', 'in_reply_to', 'is_associated',
# 'is_delivery_receipt_requested', 'is_draft', 'is_from_me', 'is_read',
# 'is_read_receipt_requested', 'is_resend', 'is_response_requested',
# 'is_submitted', 'is_unmodified', 'item_class', 'last_modified_name',
# 'last_modified_time', 'mark_as_junk', 'message_id', 'mime_content',
# 'move', 'move_to_trash', 'parent_folder_id', 'received_by',
# 'received_representing', 'references', 'refresh', 'register',
# 'reminder_due_by', 'reminder_is_set', 'reminder_message_data',
# 'reminder_minutes_before_start', 'remove_field', 'reply', 'reply_all',
# 'reply_to', 'request_tag', 'response_objects', 'response_tag', 'save',
# 'send', 'send_and_save', 'sender', 'sensitivity', 'size',
# 'soft_delete', 'subject', 'supported_fields', 'text_body', 'to_id',
# 'to_recipients', 'to_xml', 'unique_body', 'validate_field',
# 'web_client_edit_form_query_string', 'web_client_read_form_query_string']

def get_message_properties(message):

    sent = message.datetime_sent
    received = message.datetime_received

    # Collect essential message properties
    result = {'id': message.id,
              'sender': message.sender,
              'subject': message.subject,
              'recipients': message.to_recipients,
              'size': message.size,
              'sent': sent,
              'datetime_sent':
                  f'{sent.month}-{sent.day}-{sent.year}',
              'datetime_received':
                  f'{received.month}-{received.day}-{received.year}'}

    return result


# -----------------------------------------------------------------

def get_message_content(message):

    # Collect essential message properties
    result = {'id': message.id,
              'text': message.text_body,
              'html': message.body}

    return result


# ------------------------------------------------------------------
# Enhanced Hotmail Email Retrieval Function
# ------------------------------------------------------------------

def retrieve_hotmail_emails_from_sender(sender_email: str,
                                        access_token: str | None = None,
                                        folder: str = "inbox",
                                        max_emails: int = 50,
                                        include_content: bool = False,
                                        date_filter: str | None= None) -> list:
    """
    Retrieve Hotmail emails from a specific sender using Microsoft Graph API.

    Args:
        sender_email (str): Email address of the sender to filter by
        access_token (str, optional): Graph API access token. If None,
            will attempt to get one
        folder (str): Folder to search in (default: "inbox")
        max_emails (int): Maximum number of emails to retrieve (default: 50)
        include_content (bool): Whether to include full email content
            (default: False)
        date_filter (str): Optional date filter in ISO format
            (e.g., "2023-01-01T00:00:00Z")

    Returns:
        list: List of email messages from the specified sender

    Raises:
        Exception: If authentication fails or API request fails
    """

    # Get access token if not provided
    if not access_token:
        try:
            access_token = get_token_silent()
        except Exception as e:
            print(f"Failed to get access token: {e}")
            raise

    # Build the Graph API URL
    if folder.lower() == "inbox":
        url = "https://graph.microsoft.com/v1.0/me/messages"
    else:
        # For other folders, you might need to get folder ID first
        url = (f"https://graph.microsoft.com/v1.0/me/mailFolders/"
               f"{folder}/messages")

    headers = {"Authorization": f"Bearer {access_token}"}

    # Build filter parameters
    filters = [f"from/emailAddress/address eq '{sender_email}'"]

    if date_filter:
        filters.append(f"receivedDateTime ge {date_filter}")

    # Select fields to retrieve
    if include_content:
        select_fields = ("id,subject,from,receivedDateTime,body,"
                         "bodyPreview,hasAttachments,importance")
    else:
        select_fields = ("id,subject,from,receivedDateTime,"
                         "bodyPreview,hasAttachments,importance")

    params = {
        "$filter": " and ".join(filters),
        "$select": select_fields,
        "$top": min(max_emails, 1000),  # Graph API has limits
        "$orderby": "receivedDateTime desc"
    }

    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()

        emails = response.json().get("value", [])

        # Format the response for better usability
        formatted_emails = []
        for email in emails:
            from_data = email.get("from", {}).get("emailAddress", {})
            formatted_email = {
                "id": email.get("id"),
                "subject": email.get("subject"),
                "sender": from_data.get("address"),
                "sender_name": from_data.get("name"),
                "received_date": email.get("receivedDateTime"),
                "preview": email.get("bodyPreview"),
                "has_attachments": email.get("hasAttachments", False),
                "importance": email.get("importance", "normal")
            }

            if include_content:
                body_data = email.get("body", {})
                formatted_email["body"] = body_data.get("content", "")
                formatted_email["body_type"] = body_data.get("contentType",
                                                             "text")

            formatted_emails.append(formatted_email)

        return formatted_emails

    except requests.exceptions.RequestException as e:
        print(f"Error retrieving emails: {e}")
        raise
    except Exception as e:
        print(f"Unexpected error: {e}")
        raise


# ------------------------------------------------------------------
# Quick function to get emails from sender (simplified)
# ------------------------------------------------------------------

def get_emails_from_sender(sender_email: str, max_emails: int = 25) -> list:
    """
    Simplified function to quickly get emails from a specific sender.

    Args:
        sender_email (str): Email address of the sender
        max_emails (int): Maximum number of emails to retrieve (default: 25)

    Returns:
        list: List of email messages
    """
    try:
        return retrieve_hotmail_emails_from_sender(
            sender_email=sender_email,
            max_emails=max_emails,
            include_content=False
        )
    except Exception as e:
        print(f"Error getting emails from {sender_email}: {e}")
        return []


# ------------------------------------------------------------------
# Example usage function
# ------------------------------------------------------------------

def example_usage():
    """
    Example of how to use the Hotmail email retrieval functions.

    Before running this, make sure you have set up the following environment
    variables in your .env file:

    GRAPH_READER_CLIENT_ID=your_app_client_id
    GRAPH_READER_TENANT=consumers  (for personal accounts)
    GRAPH_READER_AUTHORITY=https://login.microsoftonline.com/consumers
    GRAPH_READER_SCOPES=https://graph.microsoft.com/Mail.Read
    DATA_DIR=path_to_your_data_directory
    """

    # Example 1: Get recent emails from a specific sender
    sender = "example@gmail.com"
    emails = get_emails_from_sender(sender, max_emails=10)

    print(f"Found {len(emails)} emails from {sender}")
    for email in emails[:3]:  # Show first 3
        print(f"- {email['subject']} ({email['received_date']})")

    # Example 2: Get emails with full content from last week
    from datetime import datetime, timedelta

    week_ago = (datetime.now() - timedelta(days=7)).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )

    detailed_emails = retrieve_hotmail_emails_from_sender(
        sender_email="important@company.com",
        max_emails=5,
        include_content=True,
        date_filter=week_ago
    )

    print("\nDetailed emails from last week:")
    for email in detailed_emails:
        print(f"Subject: {email['subject']}")
        print(f"From: {email['sender_name']} <{email['sender']}>")
        print(f"Date: {email['received_date']}")
        print(f"Preview: {email['preview'][:100]}...")
        if email.get('body'):
            print(f"Body length: {len(email['body'])} characters")
        print("-" * 50)


# ****************************************************************
# End of File
# ****************************************************************
