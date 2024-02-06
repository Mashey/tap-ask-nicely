from os import pipe
from requests.models import cookiejar_from_dict
import singer
from singer import Transformer, metadata
from tap_ask_nicely.client import AskNicelyClient
from tap_ask_nicely.streams import STREAMS
from tap_ask_nicely.utils import AuditLogs, SendgridMessenger, SlackMessenger, GmailMessenger
from datetime import date, datetime
import time

LOGGER = singer.get_logger()


def sync(config, state, catalog):
    client = AskNicelyClient(config)

    run_id = int(time.time())
    pipeline_start = datetime.now().strftime("%Y-%m-%d, %H:%M:%S")
    pipeline_start_time = time.perf_counter()
    stream_comments = []
    total_records = 0

    with Transformer() as transformer:
        for stream in catalog.get_selected_streams(state):
            batch_start = datetime.now().strftime("%Y-%m-%d, %H:%M:%S")
            start_time = time.perf_counter()
            record_count = 0

            tap_stream_id = stream.tap_stream_id
            stream_obj = STREAMS[tap_stream_id](client, state, config)
            replication_key = stream_obj.replication_key
            stream_schema = stream.schema.to_dict()
            stream_metadata = metadata.to_map(stream.metadata)

            LOGGER.info("Staring sync for stream: %s", tap_stream_id)

            state = singer.set_currently_syncing(state, tap_stream_id)
            singer.write_state(state)

            singer.write_schema(
                tap_stream_id,
                stream_schema,
                stream_obj.key_properties,
                stream.replication_key,
            )

            try:
                for record in stream_obj.sync():
                    transformed_record = transformer.transform(
                        record, stream_schema, stream_metadata
                    )
                    singer.write_record(
                        tap_stream_id,
                        transformed_record,
                    )
                    record_count += 1
                    total_records += 1

                if replication_key != "":
                    state = singer.write_bookmark(
                        state, tap_stream_id, replication_key, record[replication_key]
                    )
                    singer.write_state(state, tap_stream_id)

                batch_stop = datetime.now().strftime("%Y-%m-%d, %H:%M:%S")
                AuditLogs.write_audit_log(
                    run_id=run_id,
                    stream_name=tap_stream_id,
                    batch_start=batch_start,
                    batch_end=batch_stop,
                    records_synced=record_count,
                    run_time=(time.perf_counter() - start_time),
                )

            except Exception as e:
                stream_comments.append(f"{tap_stream_id.upper}: {e}")
                batch_stop = datetime.now().strftime("%Y-%m-%d, %H:%M:%S")
                AuditLogs.write_audit_log(
                    run_id=run_id,
                    stream_name=tap_stream_id,
                    batch_start=batch_start,
                    batch_end=batch_stop,
                    records_synced=record_count,
                    run_time=(time.perf_counter() - start_time),
                    comments=e,
                )

    state = singer.set_currently_syncing(state, None)
    singer.write_state(state)

    # Create dictionary for data sync info
    notification_data = {
        "run_id": run_id,
        "start_time": pipeline_start,
        "run_time": (time.perf_counter() - pipeline_start_time),
        "record_count": total_records,
        "comments": '\n'.join(stream_comments),
        "status": ""
    }

    # Comment out for local runs
    if config.get("slack_notifications"):
        SlackMessenger.send_message(
            run_id=run_id,
            start_time=pipeline_start,
            run_time=(time.perf_counter() - pipeline_start_time),
            record_count=total_records,
            comments='\n'.join(stream_comments),
        )

    if config.get("email_notifications"):
        sg = SendgridMessenger(notification_data)
        sg.send_message()
