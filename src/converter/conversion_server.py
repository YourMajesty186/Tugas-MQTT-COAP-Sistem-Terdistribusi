"""Bridge MQTT messages to CoAP and CoAP resources to MQTT."""

from __future__ import annotations

import argparse
import asyncio
import csv
import json
import logging
import math
import threading
import time
from pathlib import Path
from typing import Any

import aiocoap
from aiocoap import Context
from paho.mqtt import client as mqtt


LOGGER = logging.getLogger("conversion_server")
CONVERTER_LOG_FIELDS = (
    "id",
    "direction",
    "receive_ts",
    "convert_done_ts",
    "conversion_delay_ms",
    "status",
)


class MessageValidationError(ValueError):
    """Raised when a conversion message does not match the project contract."""


def coap_uri(host: str, port: int, resource: str) -> str:
    """Build a CoAP URI from a host, port, and resource path."""
    path = resource.strip("/")
    if not path:
        raise ValueError("CoAP resource path must not be empty")
    return f"coap://{host}:{port}/{path}"


def decode_message(raw_payload: bytes, expected_direction: str) -> dict[str, Any]:
    """Decode and validate a JSON envelope without changing its fields."""
    try:
        message = json.loads(raw_payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise MessageValidationError(f"payload is not valid UTF-8 JSON: {exc}") from exc

    if not isinstance(message, dict):
        raise MessageValidationError("message must be a JSON object")

    expected_source = {
        "mqtt_to_coap": "mqtt_publisher",
        "coap_to_mqtt": "coap_post_client",
    }.get(expected_direction)
    if expected_source is None:
        raise ValueError(f"unsupported conversion direction: {expected_direction}")

    if message.get("direction") != expected_direction:
        raise MessageValidationError("message direction does not match its input path")
    if message.get("source") != expected_source:
        raise MessageValidationError("message source does not match its input path")
    if not isinstance(message.get("id"), str) or not message["id"].strip():
        raise MessageValidationError("message id must be a non-empty string")
    if type(message.get("sequence")) is not int or message["sequence"] < 1:
        raise MessageValidationError("sequence must be a positive integer")
    sent_ts = message.get("sent_ts")
    if isinstance(sent_ts, bool) or not isinstance(sent_ts, (int, float)):
        raise MessageValidationError("sent_ts must be a number")
    if not math.isfinite(sent_ts):
        raise MessageValidationError("sent_ts must be finite")
    if not isinstance(message.get("payload"), str):
        raise MessageValidationError("payload must be a string")
    payload_size = message.get("payload_size_bytes")
    if type(payload_size) is not int or payload_size != len(message["payload"].encode("utf-8")):
        raise MessageValidationError("payload_size_bytes does not match the payload")

    return message


class CsvEventLog:
    """Append conversion events using the CSV schema documented in README."""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()

    def append(
        self,
        message_id: str,
        direction: str,
        receive_ts: float,
        convert_done_ts: float,
        status: str,
    ) -> None:
        row = {
            "id": message_id,
            "direction": direction,
            "receive_ts": receive_ts,
            "convert_done_ts": convert_done_ts,
            "conversion_delay_ms": max(0.0, (convert_done_ts - receive_ts) * 1000),
            "status": status,
        }
        with self._lock:
            write_header = not self.path.exists() or self.path.stat().st_size == 0
            with self.path.open("a", newline="", encoding="utf-8") as file:
                writer = csv.DictWriter(file, fieldnames=CONVERTER_LOG_FIELDS)
                if write_header:
                    writer.writeheader()
                writer.writerow(row)


class ConversionService:
    """Owns both translation directions and the CoAP polling loop."""

    def __init__(
        self,
        config: dict[str, Any],
        coap_context: Any,
        coap_module: Any,
        coap_error_type: type[Exception],
        mqtt_client: Any,
        poll_interval_sec: float,
        event_log: Any,
    ) -> None:
        self.config = config
        self.coap_context = coap_context
        self.coap = coap_module
        self.coap_error_type = coap_error_type
        self.mqtt = mqtt_client
        self.poll_interval_sec = poll_interval_sec
        self.event_log = event_log
        self.last_id: str | None = None

    async def forward_mqtt_to_coap(self, raw_payload: bytes, receive_ts: float) -> None:
        direction = "mqtt_to_coap"
        try:
            message = decode_message(raw_payload, direction)
        except MessageValidationError as exc:
            self._log_invalid(raw_payload, direction, receive_ts, exc)
            return

        coap_config = self.config["coap"]
        request = self.coap.Message(
            code=self.coap.POST,
            uri=coap_uri(
                coap_config["server_host"],
                coap_config["server_port"],
                coap_config["post_resource"],
            ),
            payload=raw_payload,
        )
        try:
            response = await self.coap_context.request(request).response
        except (self.coap_error_type, OSError, asyncio.TimeoutError):
            LOGGER.exception("CoAP POST failed for id=%s", message["id"])
            raise

        if not response.code.is_successful():
            error = RuntimeError(f"CoAP POST returned unsuccessful code {response.code}")
            LOGGER.error("%s for id=%s", error, message["id"])
            raise error

        self.event_log.append(
            message["id"], direction, receive_ts, time.time(), "FORWARDED"
        )
        LOGGER.info("forwarded id=%s direction=%s", message["id"], direction)

    async def _forward_coap_message(self, raw_payload: bytes, receive_ts: float) -> None:
        direction = "coap_to_mqtt"
        try:
            message = decode_message(raw_payload, direction)
        except MessageValidationError as exc:
            self._log_invalid(raw_payload, direction, receive_ts, exc)
            return

        message_id = message["id"]
        if message_id == self.last_id:
            self.event_log.append(
                message_id, direction, receive_ts, time.time(), "SKIPPED_DUPLICATE"
            )
            LOGGER.info("skipped duplicate id=%s direction=%s", message_id, direction)
            return

        topic = self.config["mqtt"]["topic_coap_to_mqtt"]
        result = self.mqtt.publish(topic, raw_payload, qos=1)
        if result.rc != mqtt.MQTT_ERR_SUCCESS:
            error = RuntimeError(
                f"MQTT publish failed for id={message_id} with code {result.rc}"
            )
            LOGGER.error("%s", error)
            raise error

        self.last_id = message_id
        self.event_log.append(
            message_id, direction, receive_ts, time.time(), "FORWARDED"
        )
        LOGGER.info("forwarded id=%s direction=%s", message_id, direction)

    async def poll_coap_to_mqtt(self) -> None:
        coap_config = self.config["coap"]
        uri = coap_uri(
            coap_config["server_host"],
            coap_config["server_port"],
            coap_config["get_resource"],
        )
        while True:
            try:
                response = await self.coap_context.request(
                    self.coap.Message(code=self.coap.GET, uri=uri)
                ).response
                if response.code.is_successful() and response.payload:
                    receive_ts = time.time()
                    await self._forward_coap_message(response.payload, receive_ts)
                elif not response.code.is_successful():
                    LOGGER.warning("CoAP GET returned unsuccessful code %s", response.code)
            except (self.coap_error_type, OSError, asyncio.TimeoutError):
                LOGGER.exception("CoAP GET failed for uri=%s", uri)
            await asyncio.sleep(self.poll_interval_sec)

    def _log_invalid(
        self, raw_payload: bytes, direction: str, receive_ts: float, error: Exception
    ) -> None:
        message_id = ""
        try:
            decoded = json.loads(raw_payload.decode("utf-8"))
            if isinstance(decoded, dict) and isinstance(decoded.get("id"), str):
                message_id = decoded["id"]
        except (UnicodeDecodeError, json.JSONDecodeError):
            pass
        self.event_log.append(
            message_id, direction, receive_ts, time.time(), "INVALID_JSON"
        )
        LOGGER.warning("rejected invalid message id=%s: %s", message_id, error)


def load_config(path: str | Path) -> dict[str, Any]:
    with Path(path).open(encoding="utf-8") as file:
        config = json.load(file)
    if not isinstance(config, dict):
        raise ValueError("configuration root must be a JSON object")
    for section in ("mqtt", "coap"):
        if not isinstance(config.get(section), dict):
            raise ValueError(f"configuration is missing the {section!r} section")
    return config


async def run(config: dict[str, Any], log_path: Path) -> None:
    coap_context = await Context.create_client_context()
    event_log = CsvEventLog(log_path)
    mqtt_config = config["mqtt"]
    coap_config = config["coap"]

    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    service = ConversionService(
        config,
        coap_context,
        aiocoap,
        aiocoap.error.Error,
        client,
        coap_config.get("poll_interval_sec", 1.0),
        event_log,
    )
    loop = asyncio.get_running_loop()

    def on_connect(
        mqtt_client: mqtt.Client,
        _userdata: Any,
        _flags: Any,
        reason_code: Any,
        _properties: Any,
    ) -> None:
        if reason_code.is_failure:
            LOGGER.error("MQTT connection failed: %s", reason_code)
            return
        result, _mid = mqtt_client.subscribe(
            mqtt_config["topic_mqtt_to_coap"], qos=1
        )
        if result != mqtt.MQTT_ERR_SUCCESS:
            LOGGER.error("MQTT subscribe failed with code %s", result)

    def on_message(
        _mqtt_client: mqtt.Client, _userdata: Any, message: mqtt.MQTTMessage
    ) -> None:
        receive_ts = time.time()
        future = asyncio.run_coroutine_threadsafe(
            service.forward_mqtt_to_coap(message.payload, receive_ts), loop
        )

        def report_failure(completed: Any) -> None:
            exception = completed.exception()
            if exception is not None:
                LOGGER.error(
                    "MQTT-to-CoAP conversion failed",
                    exc_info=(type(exception), exception, exception.__traceback__),
                )

        future.add_done_callback(report_failure)

    client.on_connect = on_connect
    client.on_message = on_message

    try:
        client.connect(
            mqtt_config["broker_host"], mqtt_config["broker_port"], keepalive=60
        )
        client.loop_start()
        await service.poll_coap_to_mqtt()
    finally:
        client.loop_stop()
        client.disconnect()
        await coap_context.shutdown()


def main() -> None:
    project_root = Path(__file__).resolve().parents[2]
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--config",
        type=Path,
        default=project_root / "config" / "config.json",
        help="path to the shared project configuration",
    )
    parser.add_argument(
        "--log",
        type=Path,
        default=project_root / "results" / "raw_logs" / "converter_log.csv",
        help="path to converter_log.csv",
    )
    args = parser.parse_args()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    try:
        asyncio.run(run(load_config(args.config), args.log))
    except KeyboardInterrupt:
        LOGGER.info("conversion server stopped")


if __name__ == "__main__":
    main()
