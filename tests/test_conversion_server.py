import asyncio
import csv
import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from src.converter.conversion_server import (
    CONVERTER_LOG_FIELDS,
    ConversionService,
    CsvEventLog,
    MessageValidationError,
    coap_uri,
    decode_message,
)


def make_message(
    direction: str = "mqtt_to_coap",
    source: str = "mqtt_publisher",
    payload: str = "test payload",
) -> dict[str, object]:
    return {
        "id": "test-001",
        "sequence": 1,
        "sent_ts": 1732611900.123456,
        "source": source,
        "direction": direction,
        "payload_size_bytes": len(payload.encode("utf-8")),
        "payload": payload,
    }


class FakeCoAP:
    POST = "POST"
    GET = "GET"

    class error:
        class Error(Exception):
            pass

    @staticmethod
    def Message(**kwargs: object) -> SimpleNamespace:
        return SimpleNamespace(**kwargs)


class FakeMQTTClient:
    def __init__(self) -> None:
        self.published: list[tuple[str, bytes, int]] = []

    def publish(self, topic: str, payload: bytes, qos: int) -> SimpleNamespace:
        self.published.append((topic, payload, qos))
        return SimpleNamespace(rc=0)


class ConversionServerTests(unittest.TestCase):
    def test_decodes_valid_envelope_and_keeps_message_fields(self) -> None:
        expected = make_message()
        self.assertEqual(
            decode_message(json.dumps(expected).encode("utf-8"), "mqtt_to_coap"),
            expected,
        )

    def test_rejects_wrong_direction_source_and_payload_size(self) -> None:
        invalid_messages = (
            (make_message(direction="coap_to_mqtt"), "mqtt_to_coap"),
            (make_message(source="unexpected"), "mqtt_to_coap"),
            ({**make_message(), "payload_size_bytes": 99}, "mqtt_to_coap"),
        )
        for message, direction in invalid_messages:
            with self.subTest(message=message):
                with self.assertRaises(MessageValidationError):
                    decode_message(json.dumps(message).encode("utf-8"), direction)

    def test_csv_log_writes_documented_header_and_conversion_delay(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "converter_log.csv"
            CsvEventLog(path).append(
                "test-001", "mqtt_to_coap", 10.0, 10.025, "FORWARDED"
            )

            with path.open(newline="", encoding="utf-8") as file:
                rows = list(csv.DictReader(file))

        self.assertEqual(tuple(rows[0].keys()), CONVERTER_LOG_FIELDS)
        self.assertEqual(rows[0]["id"], "test-001")
        self.assertAlmostEqual(float(rows[0]["conversion_delay_ms"]), 25.0)

    def test_builds_coap_uri_using_resource_path(self) -> None:
        self.assertEqual(
            coap_uri("127.0.0.1", 5683, "/actuator/data"),
            "coap://127.0.0.1:5683/actuator/data",
        )


class ConversionServiceTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.config = {
            "mqtt": {"topic_coap_to_mqtt": "iot/actuator/coap2mqtt"},
            "coap": {
                "server_host": "127.0.0.1",
                "server_port": 5683,
                "post_resource": "actuator/data",
                "get_resource": "sensor/data",
                "poll_interval_sec": 1,
            },
        }
        self.mqtt = FakeMQTTClient()
        self.events: list[tuple[object, ...]] = []
        self.event_log = SimpleNamespace(append=lambda *row: self.events.append(row))

    def make_service(self, coap_context: object) -> ConversionService:
        return ConversionService(
            self.config,
            coap_context,
            FakeCoAP,
            FakeCoAP.error.Error,
            self.mqtt,
            0,
            self.event_log,
        )

    async def test_mqtt_message_is_posted_to_configured_coap_resource_unchanged(
        self,
    ) -> None:
        raw_payload = json.dumps(make_message()).encode("utf-8")
        response = SimpleNamespace(
            code=SimpleNamespace(is_successful=lambda: True)
        )
        requests: list[SimpleNamespace] = []

        class Context:
            def request(self, request: SimpleNamespace) -> SimpleNamespace:
                requests.append(request)
                return SimpleNamespace(
                    response=asyncio.sleep(0, result=response)
                )

        service = self.make_service(Context())
        await service.forward_mqtt_to_coap(raw_payload, 10.0)

        self.assertEqual(requests[0].code, FakeCoAP.POST)
        self.assertEqual(requests[0].uri, "coap://127.0.0.1:5683/actuator/data")
        self.assertEqual(requests[0].payload, raw_payload)
        self.assertEqual(self.events[0][1], "mqtt_to_coap")
        self.assertEqual(self.events[0][4], "FORWARDED")

    async def test_coap_message_is_published_once_and_duplicates_are_skipped(
        self,
    ) -> None:
        raw_payload = json.dumps(
            make_message("coap_to_mqtt", "coap_post_client")
        ).encode("utf-8")
        service = self.make_service(None)

        await service._forward_coap_message(raw_payload, 10.0)
        await service._forward_coap_message(raw_payload, 11.0)

        self.assertEqual(
            self.mqtt.published,
            [("iot/actuator/coap2mqtt", raw_payload, 1)],
        )
        self.assertEqual(self.events[0][4], "FORWARDED")
        self.assertEqual(self.events[1][4], "SKIPPED_DUPLICATE")


if __name__ == "__main__":
    unittest.main()
