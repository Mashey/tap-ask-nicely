from datetime import datetime, timedelta
import singer
from singer import utils
from typing import Generator
from .storage import StorageHandler

LOGGER = singer.get_logger()


def increment_date_by_day(date: str) -> str:
    date_object = datetime.strptime(date, "%Y-%m-%d")
    add_day = date_object + timedelta(days=1)
    return datetime.strftime(add_day, "%Y-%m-%d")


class Stream:
    tap_stream_id = None
    key_properties = []
    replication_method = ""
    valid_replication_keys = []
    replication_key = ""
    object_type = ""

    def __init__(self, client, state, config):
        self.client = client
        self.state = state
        self.config = config
        self.storage_handler = StorageHandler(self.config.get("protocol", {}))

    def sync(self, *args, **kwargs):
        raise NotImplementedError("Sync of child class not implemented")


class Unsubscribed(Stream):
    tap_stream_id = "unsubscribed"
    key_properties = ["id"]
    object_type = "UNSUBSCRIBED"
    replication_method = "FULL_TABLE"

    def sync(self) -> Generator[dict, None, None]:
        response = self.client.fetch_unsubscribed()
        unsubscribes = response.get("data", [])
        for unsubscribe in unsubscribes:
            yield unsubscribe


class Response(Stream):
    tap_stream_id = "response"
    key_properties = ["response_id"]
    replication_key = "start_time_utc"
    valid_replication_keys = ["start_time_utc"]
    object_type = "RESPONSE"
    replication_method = "INCREMENTAL"

    def sync(self, **kwargs) -> Generator[dict, None, None]:
        page = 1
        page_size = 1000
        response_length = page_size
        start_time_utc = singer.get_bookmark(
            self.state,
            self.tap_stream_id,
            self.replication_key,
            default="1970-01-01T00:00:00Z",
        )
        end_time_utc = utils.strftime(utils.now())

        contact_ids = set()
        while response_length >= page_size:
            res = self.client.fetch_responses(
                page, page_size, start_time_utc, end_time_utc
            )
            records = res.get("data", [])
            for record in records:
                record["sent"] = datetime.fromtimestamp(int(record["sent"]))
                record["opened"] = datetime.fromtimestamp(int(record["opened"]))
                record["responded"] = datetime.fromtimestamp(int(record["responded"]))
                record["lastemailed"] = datetime.fromtimestamp(int(record["lastemailed"]))
                record["created"] = datetime.fromtimestamp(int(record["created"]))
                yield record
                contact_ids.add(record["contact_id"])
            page = page + 1
            response_length = len(records)

        self.storage_handler.write_file(self.config["file_path"], list(contact_ids))
        singer.write_bookmark(
            self.state,
            self.tap_stream_id,
            self.replication_key,
            end_time_utc,
        )


class Contact(Stream):
    tap_stream_id = "contact"
    key_properties = ["id"]
    object_type = "CONTACT"
    replication_method = "FULL_TABLE"

    def sync(self, **kwargs) -> Generator[dict, None, None]:
        contact_ids = self.storage_handler.read_file(self.config["file_path"])
        for contact_id in contact_ids:
            response = self.client.fetch_contact(contact_id)
            yield {**response["data"], **{"customproperty_c": None}}


class SentStatistics(Stream):
    # Due to this Endpoint only returning calculations, there is no backfill
    # capabilities.
    tap_stream_id = "sent_statistics"
    key_properties = []
    object_type = "SENT_STATISTICS"
    replication_method = "FULL_TABLE"

    def sync(self) -> Generator[dict, None, None]:
        rolling_day = (
            self.config["sent_statistics_days"]
            if "sent_statistics_days" in self.config
            else 1
        )
        response = self.client.fetch_sent_statistics(rolling_history=rolling_day)
        sent_stats = [response]
        for stat in sent_stats:
            yield stat


class HistoricalStats(Stream):
    tap_stream_id = "historical_stats"
    key_properties = ["date"]
    replication_key = ""
    object_type = "HISTORICAL_STATS"
    replication_method = "INCREMENTAL"

    # Records that don't have a timestamp need a separate key
    # It can't be replication key since that is on the record itself
    bookmark_key = "last_sync_date"

    def sync(self) -> Generator[dict, None, None]:
        start_from = singer.get_bookmark(
            self.state,
            self.tap_stream_id,
            self.bookmark_key,
            default=self.config["start_date"],
        )

        while start_from != datetime.strftime(datetime.now(), "%Y-%m-%d"):
            response = self.client.fetch_historical_stats(date=start_from)
            sent_stats = response["data"]
            for stat in sent_stats:
                stat["date"] = datetime(int(stat.pop("year")), int(stat.pop("month")), int(stat.pop("day"))).strftime("%Y-%m-%d")
                yield stat
            start_from = increment_date_by_day(start_from)

        singer.write_bookmark(
            self.state, self.tap_stream_id, self.bookmark_key, start_from
        )

class NpsScore(Stream):
    tap_stream_id = "nps"
    key_properties = []
    replication_key = ""
    object_type = "NPS"
    replication_method = "FULL_TABLE"

    def sync(self) -> Generator[dict, None, None]:
        rolling_days = self.config.get('nps_days', 30)

        response = self.client.fetch_nps(rolling_days=rolling_days)
        yield response



STREAMS = {
    "response": Response,
    "contact": Contact,
    "unsubscribed": Unsubscribed,
    "sent_statistics": SentStatistics,
    "historical_stats": HistoricalStats,
    "nps": NpsScore,
}
