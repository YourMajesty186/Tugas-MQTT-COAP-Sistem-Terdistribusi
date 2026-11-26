import argparse
import csv
import json
from pathlib import Path
import time
import paho.mqtt.client as mqtt


def load_config(config_path: str) -> dict:
    path = Path(config_path)
    if not path.exists():
        return {}
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def main():
    parser = argparse.ArgumentParser(description="MQTT Subscriber - Modul Anggota A")
    parser.add_argument("--config", default="config/config.json", help="Path ke config.json")
    parser.add_argument("--broker", default=None, help="Override IP MQTT Broker")
    parser.add_argument("--port", type=int, default=None, help="Override Port MQTT Broker")
    parser.add_argument("--topic", default=None, help="Override topik subscribe")
    parser.add_argument("--log-file", default="results/raw_logs/receiver_log.csv", help="Path output CSV")
    args = parser.parse_args()

    cfg = load_config(args.config)
    mqtt_cfg = cfg.get("mqtt", cfg)

    broker = (
        args.broker
        or mqtt_cfg.get("broker_host")
        or mqtt_cfg.get("broker")
        or mqtt_cfg.get("mqtt_broker_ip", "127.0.0.1")
    )
    port = (
        args.port
        if args.port is not None
        else int(
            mqtt_cfg.get("broker_port")
            or mqtt_cfg.get("port")
            or mqtt_cfg.get("mqtt_broker_port", 1883)
        )
    )
    topic = args.topic or mqtt_cfg.get("topic_coap_to_mqtt", "iot/actuator/coap2mqtt")

    log_path = Path(args.log_file)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    file_exists = log_path.exists()

    csv_file = open(log_path, mode="a", newline="", encoding="utf-8")
    writer = csv.writer(csv_file)
    if not file_exists:
        writer.writerow([
            "id", "sequence", "direction", "sent_ts",
            "final_receive_ts", "e2e_delay_ms", "integrity_ok"
        ])

    required_fields = {"id", "sequence", "sent_ts", "source", "direction", "payload_size_bytes", "payload"}
    msg_count = 0

    def on_connect(client, userdata, flags, reason_code, properties):
        if reason_code == 0:
            print(f"[MQTT-SUB] Terhubung ke {broker}:{port} | Subscribe: '{topic}'")
            client.subscribe(topic, qos=0)
        else:
            print(f"[ERROR] Gagal konek ke broker, kode: {reason_code}")

    def on_message(client, userdata, msg):
        nonlocal msg_count
        final_receive_ts = time.time()
        raw_text = msg.payload.decode("utf-8", errors="replace")

        try:
            data = json.loads(raw_text)
            has_all_fields = required_fields.issubset(data.keys())

            msg_id = data.get("id", "UNKNOWN")
            seq = data.get("sequence", -1)
            direction = data.get("direction", "coap_to_mqtt")
            sent_ts = float(data.get("sent_ts", 0.0))
            expected_size = int(data.get("payload_size_bytes", -1))
            payload_text = str(data.get("payload", ""))

            actual_size = len(payload_text.encode("utf-8"))
            integrity_ok = has_all_fields and (actual_size == expected_size)
            e2e_delay_ms = (final_receive_ts - sent_ts) * 1000.0 if sent_ts > 0 else -1.0

            writer.writerow([
                msg_id, seq, direction, f"{sent_ts:.6f}",
                f"{final_receive_ts:.6f}", f"{e2e_delay_ms:.3f}", integrity_ok
            ])
            csv_file.flush()

            msg_count += 1
            print(f"[MQTT-SUB #{msg_count}] ID={msg_id} | Seq={seq} | E2E={e2e_delay_ms:.2f} ms | OK={integrity_ok}")

        except Exception as e:
            print(f"[ERROR] JSON tidak valid: {e}")
            writer.writerow(["INVALID_JSON", -1, "coap_to_mqtt", 0, f"{final_receive_ts:.6f}", -1, False])
            csv_file.flush()

    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="mqtt_sub_receiver")
    client.on_connect = on_connect
    client.on_message = on_message

    try:
        client.connect(broker, port, keepalive=60)
        client.loop_forever()
    except KeyboardInterrupt:
        print(f"\n[MQTT-SUB] Dihentikan. Total pesan diterima: {msg_count} -> {log_path}")
    finally:
        csv_file.close()
        client.disconnect()


if __name__ == "__main__":
    main()