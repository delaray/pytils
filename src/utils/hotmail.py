import os
import imaplib
import email
from email.header import decode_header


HOTMAIL_USER = os.environ['HOTMAIL_USER']
HOTMAIL_PWD = os.environ['HOTMAIL_PWD']


def get_emails_from_sender(username, password, sender_email, mailbox="inbox"):
    """
    Connects to your Hotmail (Outlook) IMAP server, logs in, and retrieves a list of email
    messages from the specified sender.

    Parameters:
        username (str): Your Hotmail email address.
        password (str): Your Hotmail account password.
        sender_email (str): The sender email address to filter emails.
        mailbox (str): The mailbox folder to search in (default is "inbox").

    Returns:
        list: A list of email.message.Message objects that were sent by the specified sender.
    """
    # Hotmail/Outlook IMAP server
    imap_server = "outlook.office365.com"

    try:
        # Connect securely to the IMAP server
        mail = imaplib.IMAP4_SSL(imap_server)
    except Exception as e:
        print(f"Could not connect to IMAP server: {e}")
        return []

    try:
        # Log in to your account
        mail.login(username, password)
    except imaplib.IMAP4.error as e:
        print(f"Login failed: {e}")
        return []

    # Select the specified mailbox (by default, the "inbox")
    status, messages = mail.select(mailbox)
    if status != "OK":
        print(f"Could not open mailbox: {mailbox}")
        mail.logout()
        return []

    # Search for emails from the specified sender
    # Note: The search criteria is case-insensitive on many servers.
    search_criterion = f'(FROM "{sender_email}")'
    status, data = mail.search(None, search_criterion)
    if status != "OK":
        print("Error during email search.")
        mail.logout()
        return []

    # data[0] is a space-separated string of email IDs
    email_ids = data[0].split()
    emails = []

    for e_id in email_ids:
        # Fetch the entire message (RFC822 format)
        status, msg_data = mail.fetch(e_id, '(RFC822)')
        if status != "OK":
            print(f"Error fetching email ID {e_id}")
            continue

        # The fetched data is a list of tuples; the email content is in the second element
        raw_email = msg_data[0][1]
        # Convert bytes to an email.message.Message object
        msg = email.message_from_bytes(raw_email)
        emails.append(msg)

    mail.logout()
    return emails


def test_hotmail(sender='daily_papers_digest@notifications.huggingface.com',
               user=HOTMAIL_USER, pwd=HOTMAIL_PWD):


    user_email = f'{user}.hotmail.com'
    # Replace these with your actual Hotmail credentials and the sender's email address
    USERNAME = f'{user}.hotmail.com'
    PASSWORD = pwd
    SENDER_EMAIL = sender

    messages = get_emails_from_sender(USERNAME, PASSWORD, SENDER_EMAIL)
    print(f"Found {len(messages)} email(s) from {SENDER_EMAIL}")

    # Optional: Print subject lines of the retrieved emails
    for i, msg in enumerate(messages, start=1):
        subject, encoding = decode_header(msg.get("Subject"))[0]
        if isinstance(subject, bytes):
            # Decode subject if it's a byte string
            subject = subject.decode(encoding if encoding else "utf-8", errors="replace")
        print(f"Email {i}: {subject}")


# Example usage:
if __name__ == "__main__":
    test_hotmail()
