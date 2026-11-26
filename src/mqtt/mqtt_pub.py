import argparse
import csv
import json
import os
from pathlib import Path
import time
import paho.mqtt.client as mqtt


def load_config(config_path: str) -> dict:
    path = Path(config_path)
    if not path.exists():
        return {}
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def build_payload(target_bytes: int) -> str:
    prefix = "DATA_UJI_SISTEM_TERDISTRIBUSI_"
    if target_bytes <= len(prefix):
        return prefix[:target_bytes]
    return prefix + ("A" * (target_bytes - len(prefix)))


def main():
    parser = argparse.ArgumentParser(description="MQTT Publisher - Modul Anggota A")
    parser.add_argument("--config", default="config/config.json", help="Path ke config.json")
    parser.add_argument("--broker", default=None, help="Override IP MQTT Broker")
    parser.add_argument("--port", type=int, default=None, help="Override Port MQTT Broker")
    parser.add_argument("--topic", default=None, help="Override topik publish")
    parser.add_argument("--rate", type=float, default=10.0, help="Laju pesan per detik (10 atau 20)")
    parser.add_argument("--duration", type=float, default=None, help="Durasi pengujian (detik), misal 600")
    parser.add_argument("--count", type=int, default=200, help="Jumlah pesan jika --duration tidak diisi")
    parser.add_argument("--payload-size", type=int, default=None, help="Ukuran payload dalam byte")
    parser.add_argument("--run-id", default="r1_01", help="ID sesi/pengulangan agar ID pesan unik")
    parser.add_argument("--log-file", default="results/raw_logs/sender_log.csv", help="Path output CSV")
    args = parser.parse_args()

    cfg = load_config(args.config)
    mqtt_cfg = cfg.get("mqtt", cfg)
    exp_cfg = cfg.get("experiment", cfg)

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
    topic = args.topic or mqtt_cfg.get("topic_mqtt_to_coap", "iot/sensor/mqtt2coap")
    payload_size = args.payload_size or int(exp_cfg.get("payload_size_bytes", 1024))

    log_path = Path(args.log_file)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    file_exists = log_path.exists()

    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=f"mqtt_pub_{args.run_id}")
    client.connect(broker, port, keepalive=60)
    client.loop_start()

    payload_str = build_payload(payload_size)
    actual_bytes = len(payload_str.encode("utf-8"))
    interval = 1.0 / args.rate

    csv_file = open(log_path, mode="a", newline="", encoding="utf-8")
    writer = csv.writer(csv_file)
    if not file_exists:
        writer.writerow(["id", "sequence", "direction", "sent_ts", "payload_size_bytes"])

    print(f"[MQTT-PUB] Target: {broker}:{port} | Topik: {topic}")
    print(f"[MQTT-PUB] Rate: {args.rate} msg/s | Payload: {actual_bytes} B | Run: {args.run_id}")

    seq = 0
    start_time = time.time()
    next_send = start_time

    try:
        while True:
            now = time.time()
            if args.duration is not None:
                if (now - start_time) >= args.duration:
                    break
            else:
                if seq >= args.count:
                    break

            seq += 1
            msg_id = f"msg-mqtt2coap-{args.run_id}-{seq:05d}"
            sent_ts = time.time()

            message = {
                "id": msg_id,
                "sequence": seq,
                "sent_ts": sent_ts,
                "source": "mqtt_publisher",
                "direction": "mqtt_to_coap",
                "payload_size_bytes": actual_bytes,
                "payload": payload_str,
            }

            result = client.publish(topic, json.dumps(message), qos=0)
            if result.rc != mqtt.MQTT_ERR_SUCCESS:
                print(f"[WARN] Gagal publish {msg_id}, rc={result.rc}")

            writer.writerow([msg_id, seq, "mqtt_to_coap", f"{sent_ts:.6f}", actual_bytes])
            if seq % 50 == 0:
                csv_file.flush()
                print(f"[MQTT-PUB] Terkirim {seq} pesan (terakhir: {msg_id})")

            next_send += interval
            sleep_time = next_send - time.time()
            if sleep_time > 0:
                time.sleep(sleep_time)

    except KeyboardInterrupt:
        print("\n[MQTT-PUB] Dihentikan oleh pengguna.")
    finally:
        csv_file.flush()
        csv_file.close()
        client.loop_stop()
        client.disconnect()
        elapsed = time.time() - start_time
        print(f"[MQTT-PUB] Selesai: {seq} pesan dalam {elapsed:.2f} detik -> {log_path}")


if __name__ == "__main__":
    main()