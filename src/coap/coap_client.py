import argparse
import asyncio
import csv
import json
import logging
import time
from pathlib import Path

import aiocoap

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [COAP-CLIENT] %(levelname)s: %(message)s",
)
logger = logging.getLogger(__name__)

ROOT_DIR = Path(__file__).resolve().parents[2]
CONFIG_PATH = ROOT_DIR / "config" / "config.json"
RAW_LOG_DIR = ROOT_DIR / "results" / "raw_logs"
SENDER_LOG = RAW_LOG_DIR / "sender_log.csv"
RECEIVER_LOG = RAW_LOG_DIR / "receiver_log.csv"

SENDER_HEADERS = ["id", "sequence", "direction", "sent_ts", "payload_size_bytes"]
RECEIVER_HEADERS = [
    "id",
    "sequence",
    "direction",
    "sent_ts",
    "final_receive_ts",
    "e2e_delay_ms",
    "integrity_ok",
]


def load_config() -> dict:
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def ensure_csv(path: Path, headers: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists() or path.stat().st_size == 0:
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(headers)


def append_csv(path: Path, row: list) -> None:
    with open(path, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(row)


def build_payload_string(target_bytes: int) -> str:
    prefix = "DATA_UJI_SISTEM_TERDISTRIBUSI_"
    if target_bytes <= len(prefix):
        return prefix[:target_bytes]
    return prefix + ("A" * (target_bytes - len(prefix)))


async def send_single_post(
    context: aiocoap.Context,
    uri: str,
    msg_dict: dict,
    sem: asyncio.Semaphore,
) -> None:
    async with sem:
        raw_bytes = json.dumps(msg_dict).encode("utf-8")
        request = aiocoap.Message(code=aiocoap.POST, uri=uri, payload=raw_bytes)
        try:
            response = await asyncio.wait_for(context.request(request).response, timeout=5.0)
            logger.info(
                "POST terkirim | id=%s | seq=%d | code=%s",
                msg_dict["id"],
                msg_dict["sequence"],
                response.code,
            )
        except Exception as exc:
            logger.error("POST gagal | id=%s | error=%s", msg_dict["id"], exc)


async def run_post_mode(
    cfg: dict,
    rate: float,
    count: int | None,
    duration: float | None,
    payload_size: int,
    run_id: str,
) -> None:
    coap_cfg = cfg["coap"]
    uri = f"coap://{coap_cfg['server_host']}:{coap_cfg['server_port']}/{coap_cfg['get_resource'].strip('/')}"
    ensure_csv(SENDER_LOG, SENDER_HEADERS)

    context = await aiocoap.Context.create_client_context()
    sem = asyncio.Semaphore(50)
    payload_str = build_payload_string(payload_size)
    actual_size = len(payload_str.encode("utf-8"))
    interval = 1.0 / rate

    logger.info(
        "Mulai mode POST -> %s | rate=%.2f msg/s | payload=%d bytes | run_id=%s",
        uri,
        rate,
        actual_size,
        run_id,
    )

    tasks: list[asyncio.Task] = []
    start_monotonic = time.monotonic()
    seq = 0

    try:
        while True:
            if count is not None and seq >= count:
                break
            if duration is not None and (time.monotonic() - start_monotonic) >= duration:
                break

            seq += 1
            sent_ts = time.time()
            msg_id = f"msg-coap2mqtt-{run_id}-{seq:04d}"
            msg_dict = {
                "id": msg_id,
                "sequence": seq,
                "sent_ts": sent_ts,
                "source": "coap_post_client",
                "direction": "coap_to_mqtt",
                "payload_size_bytes": actual_size,
                "payload": payload_str,
            }

            append_csv(
                SENDER_LOG,
                [msg_id, seq, "coap_to_mqtt", f"{sent_ts:.6f}", actual_size],
            )
            tasks.append(asyncio.create_task(send_single_post(context, uri, msg_dict, sem)))

            next_tick = start_monotonic + (seq * interval)
            sleep_time = next_tick - time.monotonic()
            if sleep_time > 0:
                await asyncio.sleep(sleep_time)

        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
    finally:
        await context.shutdown()
        logger.info("Mode POST selesai. Total pesan dikirim: %d", seq)


async def run_get_mode(cfg: dict, poll_interval: float, duration: float | None) -> None:
    coap_cfg = cfg["coap"]
    uri = f"coap://{coap_cfg['server_host']}:{coap_cfg['server_port']}/{coap_cfg['post_resource'].strip('/')}"
    ensure_csv(RECEIVER_LOG, RECEIVER_HEADERS)

    context = await aiocoap.Context.create_client_context()
    seen_ids: set[str] = set()
    start_monotonic = time.monotonic()

    logger.info("Mulai mode GET verifikasi <- %s | poll=%.3fs", uri, poll_interval)

    try:
        while True:
            if duration is not None and (time.monotonic() - start_monotonic) >= duration:
                break

            request = aiocoap.Message(code=aiocoap.GET, uri=uri)
            try:
                response = await asyncio.wait_for(context.request(request).response, timeout=3.0)
                if response.code == aiocoap.CONTENT and response.payload:
                    final_receive_ts = time.time()
                    data = json.loads(response.payload.decode("utf-8"))
                    msg_id = str(data.get("id", ""))

                    if msg_id and msg_id not in seen_ids:
                        seen_ids.add(msg_id)
                        seq = int(data.get("sequence", -1))
                        direction = str(data.get("direction", "mqtt_to_coap"))
                        sent_ts = float(data.get("sent_ts", 0.0))
                        expected_size = int(data.get("payload_size_bytes", -1))
                        payload_str = str(data.get("payload", ""))

                        e2e_delay_ms = (final_receive_ts - sent_ts) * 1000.0
                        integrity_ok = (
                            bool(msg_id)
                            and seq > 0
                            and len(payload_str.encode("utf-8")) == expected_size
                        )

                        append_csv(
                            RECEIVER_LOG,
                            [
                                msg_id,
                                seq,
                                direction,
                                f"{sent_ts:.6f}",
                                f"{final_receive_ts:.6f}",
                                f"{e2e_delay_ms:.3f}",
                                integrity_ok,
                            ],
                        )
                        logger.info(
                            "GET terverifikasi | id=%s | e2e=%.2f ms | integrity=%s",
                            msg_id,
                            e2e_delay_ms,
                            integrity_ok,
                        )
            except (json.JSONDecodeError, ValueError) as exc:
                logger.warning("Payload GET tidak valid: %s", exc)
            except Exception as exc:
                logger.debug("GET belum siap/timeout: %s", exc)

            await asyncio.sleep(poll_interval)
    finally:
        await context.shutdown()
        logger.info("Mode GET selesai. Total pesan unik terverifikasi: %d", len(seen_ids))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="CoAP POST/GET Client")
    parser.add_argument("--mode", choices=["post", "get"], required=True)
    parser.add_argument("--rate", type=float, default=None, help="Laju pengiriman (pesan/detik)")
    parser.add_argument("--count", type=int, default=None, help="Jumlah pesan yang dikirim (mode post)")
    parser.add_argument("--duration", type=float, default=None, help="Durasi jalan dalam detik")
    parser.add_argument("--payload-size", type=int, default=None, help="Ukuran payload dalam byte")
    parser.add_argument("--run-id", type=str, default="run1", help="Penanda pengulangan uji")
    parser.add_argument("--poll", type=float, default=0.05, help="Interval baca mode GET (detik)")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    config = load_config()

    try:
        if args.mode == "post":
            rate_val = args.rate if args.rate is not None else float(config["experiment"]["rates_msg_per_sec"][0])
            size_val = (
                args.payload_size
                if args.payload_size is not None
                else int(config["experiment"]["payload_size_bytes"])
            )
            count_val = args.count if (args.count is not None or args.duration is not None) else 200
            asyncio.run(run_post_mode(config, rate_val, count_val, args.duration, size_val, args.run_id))
        else:
            asyncio.run(run_get_mode(config, args.poll, args.duration))
    except KeyboardInterrupt:
        logger.info("CoAP Client dihentikan oleh pengguna.")