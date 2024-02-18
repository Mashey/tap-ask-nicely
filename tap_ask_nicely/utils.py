import requests
import singer
import logging as LOGGER
from singer import Transformer, metadata
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Content, Email, Mail
import smtplib, ssl
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

## Env file for testing - Change to Config for Prod
## Keep an Env for a single key in the meltano
import os
import json
from datetime import datetime


class SlackMessenger:
    slack_base = "https://hooks.slack.com/services"
    headers = {"Content-Type": "application/json"}

    def send_message(
        run_id: int,
        start_time: str,
        run_time: int,
        record_count: int,
        comments: str = "",
        status: str = ":large_green_circle:",
    ):
        if comments != "":
            status = ":red_circle:"

        message_header = f"Tap AskNicely Update:"
        message_details = f"""Tap AskNicely Update:
        - Run Id: {run_id},
        - Status: {status},
        - Start Time: {start_time},
        - Run Time: {run_time}
        - Records Synced: {record_count},
        - Comments: {comments}"""

        json_message = {
            "text": message_details,
        }

        return requests.post(
            SlackMessenger.build_url(),
            headers=SlackMessenger.headers,
            data=json.dumps(json_message),
        )

    def build_url() -> str:
        return f'{SlackMessenger.slack_base}/{os.getenv("SLACK_WEBHOOK_ADDRESS")}'


class AuditLogs:
    def audit_schema() -> dict:
        schema_base = {
            "type": ["null", "object"],
            "additionalProperties": False,
            "properties": {
                "run_id": {"type": ["null", "integer"]},
                "stream_name": {"type": ["null", "string"]},
                "batch_start": {"type": ["null", "string"], "format": "date-time"},
                "batch_end": {"type": ["null", "string"], "format": "date-time"},
                "records_synced": {"type": ["null", "integer"]},
                "run_time": {"type": ["null", "number"]},
                "comments": {"type": ["null", "string"]},
            },
            "primary_key": ["run_id"]
        }
        return schema_base

    def schema_metadata():
        metadata = [
            {
                "breadcrumb": [],
                "metadata": {
                    "table-key-properties": [],
                    "forced-replication-method": "FULL_TABLE",
                    "inclusion": "available",
                },
            },
            {
                "breadcrumb": ["properties", "run_id"],
                "metadata": {"inclusion": "available"},
            },
            {
                "breadcrumb": ["properties", "stream_stream"],
                "metadata": {"inclusion": "available"},
            },
            {
                "breadcrumb": ["properties", "batch_start"],
                "metadata": {"inclusion": "available"},
            },
            {
                "breadcrumb": ["properties", "batch_end"],
                "metadata": {"inclusion": "available"},
            },
            {
                "breadcrumb": ["properties", "records_synced"],
                "metadata": {"inclusion": "available"},
            },
            {
                "breadcrumb": ["properties", "run_time"],
                "metadata": {"inclusion": "available"},
            },
            {
                "breadcrumb": ["properties", "comments"],
                "metadata": {"inclusion": "available"},
            },
        ]

        return metadata

    def audit_record(
        run_id: int,
        stream_name: str,
        batch_start: str,
        run_time: int,
        batch_end: str = datetime.now(),
        records_synced: int = 0,
        comments: str = "",
    ) -> dict:

        audit = {
            "run_id": run_id,
            "stream_name": stream_name,
            "batch_start": batch_start,
            "batch_end": batch_end,
            "records_synced": records_synced,
            "run_time": run_time,
            "comments": comments,
        }
        return audit

    def write_audit_log(
        run_id: int,
        stream_name: str,
        batch_start: str,
        batch_end: str = datetime.now(),
        records_synced: int = 0,
        run_time=int,
        comments: str = "",
    ):
        singer.write_schema(
            "audit_log",
            AuditLogs.audit_schema(),
            [],
            "",
        )
        audit_log = Transformer().transform(
            AuditLogs.audit_record(
                run_id=run_id,
                stream_name=stream_name,
                batch_start=batch_start,
                batch_end=batch_end,
                records_synced=records_synced,
                run_time=run_time,
                comments=comments,
            ),
            AuditLogs.audit_schema(),
            metadata.to_map(AuditLogs.schema_metadata()),
        )
        singer.write_record(
            "audit_log",
            audit_log,
        )


class EmailMessenger:
    def __init__(self, sync_data: dict) -> None:
        self.data = sync_data
        self.sender_email = os.getenv("EMAIL_ORIGIN")
        self.receiver_email = os.getenv("EMAIL_DESTINATION")

    status_circles = {"green": "🟢", "yellow": "🟡", "red": "🔴"}

    def email_message(self):
        status = self.sync_status()
        comments = self.sync_comments()

        text_email = f"""\
        Hi,
        
        It's the Mashey team with a data pipeline update!

        Run ID: {self.data["run_id"]}
        Batch Start: {self.data["start_time"]}
        Run Time: {self.data["run_time"]}
        Records Synced: {self.data["record_count"]}
        Status: {status}
        Comments: {comments}
        """

        html_email = f"""\
        <html>
        <body>
            <p>Hi,<br>
            <br>
            It's the Mashey team with a data pipeline update!<br>
            <ul>
                <li>Run ID: {self.data["run_id"]}</li>
                <li>Batch Start: {self.data["start_time"]}</li>
                <li>Run Time: {self.data["run_time"]}</li>
                <li>Records Synced: {self.data["record_count"]}</li>
                <li>Status: {status}</li>
                <li>Comments: {comments}</li>
            </ul>
            <a href="http://www.mashey.com">Mashey</a><br>
            </p>
        </body>
        </html>
        """

        return text_email, html_email

    def email_subject(self):
        if self.data["comments"] != "":
            subject = f"Mashey | Data Sync | {self.status_circles['red']}"
            return subject
        else:
            subject = f"Mashey | Data Sync | {self.status_circles['green']}"
            return subject

    def sync_status(self):
        if self.data["comments"] != "":
            status = f"Failure | {self.status_circles['red']}"
            return status
        else:
            status = f"Success | {self.status_circles['green']}"
            return status

    def sync_comments(self):
        if self.data["comments"] != "":
            return self.data["comments"]
        else:
            comments = "The connector is performing as expected!"
            return comments


class SendgridMessenger(EmailMessenger):
    def create_message(self):
        text_email, html_email = self.email_message()

        message = Mail(
            from_email=self.sender_email,
            to_emails=self.receiver_email,
            subject=self.email_subject(),
            html_content=html_email,
        )

        return message

    def send_message(self):
        try:
            sg = SendGridAPIClient(os.getenv("SENDGRID_API_KEY"))
            message = self.create_message()
            response = sg.send(message)

            LOGGER.info(response.status_code)
            LOGGER.info(response.body)
            LOGGER.info(response.headers)

            return "Email sent successfully."
        except Exception as error:
            LOGGER.warning(f"There was an issue sending the email: {error}")

            return f"There was an issue sending the email: {error}"


class GmailMessenger(EmailMessenger):
    def create_message(self):
        text_email, html_email = self.email_message()
        message = MIMEMultipart("alternative")
        message["Subject"] = self.email_subject()
        message["From"] = self.sender_email
        message["To"] = self.receiver_email

        mime_text = MIMEText(text_email, "plain")
        mime_html = MIMEText(html_email, "html")

        message.attach(mime_text)
        message.attach(mime_html)

        return message

    def send_message(self):
        message = self.create_message()

        context = ssl.create_default_context()
        with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
            try:
                server.login(self.sender_email, os.getenv("EMAIL_PW"))
                server.sendmail(
                    self.sender_email,
                    self.receiver_email,
                    message.as_string(),
                )
                server.quit()

                return "Email sent successfully."
            except Exception as error:
                LOGGER.error(f"There was an issue sending the email: {error}")

                return f"There was an issue sending the email: {error}"
