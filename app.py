import os
import json
import psycopg2
from datetime import datetime, timedelta, timezone

from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail, Email, To, Content

# Load environment variables from .env file for local development


# Environment variables
api_key = os.environ.get('SENDGRID_API_KEY')
DATABASE_URL = os.environ.get('DATABASE_URL')

# Initialize the SendGridAPIClient with the API key
sg = SendGridAPIClient(api_key)

# Define your query parameters here, including the date range
current_time = datetime.now(timezone.utc)
start_date = current_time - timedelta(minutes=60)
end_date = current_time
start_date_str = start_date.strftime('%Y-%m-%dT%H:%M:%S.000Z')
end_date_str = end_date.strftime('%Y-%m-%dT%H:%M:%S.000Z')
params = {
    'query': f'last_event_time BETWEEN TIMESTAMP "{start_date_str}" AND TIMESTAMP "{end_date_str}"',
    'limit': 200
}

# Function to insert data into PostgreSQL, ensuring msg_id is unique, with feedback on skipped insertions
def insert_into_sql(data):
    with psycopg2.connect(DATABASE_URL, sslmode='require') as conn:
        cursor = conn.cursor()
        for message in data:
            cursor.execute("SELECT 1 FROM email_data WHERE msg_id = %s", (message.get("msg_id"),))
            if cursor.fetchone():
                print(f"Skipping insertion: msg_id {message.get('msg_id')} already exists.")
            else:
                insert_sql = """
                    INSERT INTO email_data 
                    (from_email, msg_id, subject, to_email, status, opens_count, clicks_count, last_event_time) 
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """
                cursor.execute(insert_sql, (
                    message.get("from_email"), 
                    message.get("msg_id"), 
                    message.get("subject"),
                    message.get("to_email"), 
                    message.get("status"), 
                    message.get("opens_count"), 
                    message.get("clicks_count"), 
                    datetime.strptime(message.get("last_event_time"), '%Y-%m-%dT%H:%M:%SZ')
                ))
                print(f"Inserted: msg_id {message.get('msg_id')}")
        conn.commit()

# Function to fetch emails with status not 'delivered'
def fetch_emails_with_status_delivered():
    emails_to_resend = []
    with psycopg2.connect(DATABASE_URL, sslmode='require') as conn:
        cursor = conn.cursor()
        select_query = """SELECT msg_id, from_email, to_email 
                        FROM email_data 
                        WHERE status <> 'delivered' 
                        AND send_email_again = FALSE
                        """
        cursor.execute(select_query)
        for row in cursor.fetchall():
            emails_to_resend.append(dict(zip(['msg_id', 'from_email', 'to_email'], row)))
    return emails_to_resend

# Function to send email
def send_email(sendgrid_api_key, from_email, to_email, subject, content):
    sg = SendGridAPIClient(sendgrid_api_key)
    from_email = Email(from_email)
    to_email = To(to_email)
    content = Content("text/plain", content)
    mail = Mail(from_email, to_email, subject, content)
    response = sg.client.mail.send.post(request_body=mail.get())

    print(f"Response status code: {response.status_code}")
    print(f"Response body: {response.headers}")
    
    new_message_id = response.headers.get('X-Message-Id')
    print(f"New Message ID: {new_message_id}")
    
    return new_message_id

def update_database_with_new_message_id(original_msg_id, new_msg_id):
    with psycopg2.connect(DATABASE_URL, sslmode='require') as conn:
        cursor = conn.cursor()
        update_sql = """
            UPDATE email_data
            SET new_message_id = %s, send_email_again = TRUE
            WHERE msg_id = %s
        """
        cursor.execute(update_sql, (new_msg_id, original_msg_id))
        conn.commit()

# Main execution
try:
    # Make the API call
    response = sg.client.messages.get(query_params=params)
    if 200 <= response.status_code < 300:
        # Convert the response body to a Python dict
        response_data = json.loads(response.body)
        messages = response_data.get("messages", [])
        insert_into_sql(messages)

        # Fetch and resend emails
        delivered_emails = fetch_emails_with_status_delivered()
        for email_info in delivered_emails:
            from_email = email_info['from_email']
            to_email = 'Anderson@moahco.com'
            original_msg_id = email_info['msg_id']  # Accessing msg_id correctly
            
            # Your email subject and content
            subject = "Resending: Your previous email subject"
            content = "This is a resend of your previous email content."
            
            # Send the email and get the new message ID
            new_message_id = send_email(api_key, from_email, to_email, subject, content)
            
            # Update the database with the new message ID and set send_email_again to 1
            if new_message_id:
                update_database_with_new_message_id(
                    original_msg_id,
                    new_message_id
                )

    else:
        print(f"Failed to retrieve messages: {response.status_code}")
        print(f"Response: {response.body}")

except Exception as e:
    print(f"An error occurred: {e}")
