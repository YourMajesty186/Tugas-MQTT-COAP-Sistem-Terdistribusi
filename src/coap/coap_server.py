import asyncio
import json
import logging
from pathlib import Path

import aiocoap
import aiocoap.resource as resource

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [COAP-SERVER] %(levelname)s: %(message)s",
)
logger = logging.getLogger(__name__)

ROOT_DIR = Path(__file__).resolve().parents[2]
CONFIG_PATH = ROOT_DIR / "config" / "config.json"


def load_config() -> dict:
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


class DataResource(resource.Resource):
    """Resource CoAP yang mendukung POST (simpan pesan terbaru) dan GET (ambil pesan terbaru)."""

    def __init__(self, name: str):
        super().__init__()
        self.name = name
        self.latest_payload: bytes = b""
        self.latest_id: str | None = None

    async def render_post(self, request: aiocoap.Message) -> aiocoap.Message:
        self.latest_payload = request.payload
        msg_id = "UNKNOWN"

        try:
            data = json.loads(request.payload.decode("utf-8"))
            msg_id = str(data.get("id", "UNKNOWN"))
            self.latest_id = msg_id
            logger.info(
                "POST /%s | stored id=%s | size=%d bytes | response=CHANGED (2.04)",
                self.name,
                msg_id,
                len(request.payload),
            )
            ack = json.dumps({"status": "stored", "id": msg_id}).encode("utf-8")
            return aiocoap.Message(code=aiocoap.CHANGED, payload=ack)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            logger.warning("POST /%s | invalid JSON: %s", self.name, exc)
            return aiocoap.Message(
                code=aiocoap.BAD_REQUEST,
                payload=b'{"status": "invalid_json"}',
            )

    async def render_get(self, request: aiocoap.Message) -> aiocoap.Message:
        if not self.latest_payload:
            logger.debug("GET /%s | empty | response=NOT_FOUND (4.04)", self.name)
            return aiocoap.Message(
                code=aiocoap.NOT_FOUND,
                payload=b'{"status": "empty"}',
            )

        logger.info(
            "GET /%s | served id=%s | response=CONTENT (2.05)",
            self.name,
            self.latest_id,
        )
        return aiocoap.Message(code=aiocoap.CONTENT, payload=self.latest_payload)


async def main() -> None:
    cfg = load_config()
    coap_cfg = cfg["coap"]
    host = coap_cfg["server_host"]
    port = int(coap_cfg["server_port"])
    post_res = coap_cfg["post_resource"].strip("/")
    get_res = coap_cfg["get_resource"].strip("/")

    root = resource.Site()
    root.add_resource(tuple(post_res.split("/")), DataResource(post_res))
    root.add_resource(tuple(get_res.split("/")), DataResource(get_res))

    await aiocoap.Context.create_server_context(root, bind=(host, port))
    logger.info("CoAP Server aktif di coap://%s:%d", host, port)
    logger.info("Resource terdaftar: /%s dan /%s", post_res, get_res)

    await asyncio.get_running_loop().create_future()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("CoAP Server dihentikan.")