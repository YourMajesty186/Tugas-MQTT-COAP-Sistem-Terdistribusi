# Replikasi Server Konversi Dua Arah MQTT ↔ CoAP

Implementasi komunikasi dua arah (interoperabilitas lapisan aplikasi) antara protokol **MQTT** dan **CoAP** pada jaringan IP konvensional menggunakan Python. Proyek ini mereplikasi gagasan utama artikel acuan *Sumadi et al., TELKOMNIKA (2021)* tanpa menggunakan perangkat keras fisik maupun jaringan SDN.

## Struktur Direktori

```text
mqtt-coap-replication/
├── config/
│   ├── config.json              # Konfigurasi terpusat IP, port, topik, resource, & rate uji
│   └── mosquitto.conf           # Konfigurasi MQTT broker (Anggota A)
├── src/
│   ├── mqtt/                    # Modul sisi MQTT (Anggota A)
│   │   ├── mqtt_pub.py          # Pengirim pesan uji MQTT (10 & 20 msg/s)
│   │   └── mqtt_sub.py          # Penerima & validator hasil konversi dari CoAP
│   ├── coap/                    # Modul sisi CoAP (Anggota B)
│   │   ├── coap_server.py       # Server CoAP UDP (resource POST & GET + simpan data)
│   │   └── coap_client.py       # Klien CoAP (mode POST pengirim & mode GET verifikator)
│   ├── converter/               # Modul otak penerjemah (Anggota C)
│   │   └── conversion_server.py # Translasi dua arah (MQTT <-> CoAP) + deduplikasi last_id
│   └── monitoring/              # Modul pemantau resource (Anggota B)
│       └── monitor_cpu.py       # Perekam CPU & RAM proses conversion_server via psutil
├── scripts/
│   ├── run_experiment.py        # Skrip orkestrasi pengujian otomatis (R1 & R2)
│   └── analyze_results.py       # Skrip pengolah CSV mentah -> CSV ringkasan & 3 grafik wajib
├── results/
│   ├── raw_logs/                # Output CSV mentah tiap skenario & pengulangan
│   ├── summary/                 # Output CSV ringkasan (keberhasilan, loss, persentil 95)
│   └── charts/                  # Output 3 grafik PNG (e2e delay, conversion delay, CPU)
├── docs/
│   └── evidence/                # Screenshot packet capture Wireshark & bukti log
├── .gitignore
├── requirements.txt
└── README.md
```

## Instalasi

1. Clone repositori dan masuk ke direktori proyek:
   ```bash
   git clone <url-repositori-kelompok>
   cd mqtt-coap-replication
   ```
2. Buat dan aktifkan *virtual environment* (disarankan), lalu instal seluruh pustaka:
   ```bash
   pip install -r requirements.txt
   ```
3. Pastikan **Mosquitto Broker** sudah terpasang pada mesin yang menjalankan sisi MQTT, lalu jalankan dengan konfigurasi proyek:
   ```bash
   mosquitto -c config/mosquitto.conf -v
   ```
4. Setelah broker dan CoAP server aktif, jalankan conversion server dari root repositori:
   ```bash
   python src/converter/conversion_server.py
   ```
   Server membaca `config/config.json` dan menulis log ke `results/raw_logs/converter_log.csv`.

---

## Kontrak Teknis (Wajib Diikuti Seluruh Modul)

### 1. Endpoint & Alur Komunikasi
Seluruh modul wajib membaca alamat IP, port, topik, dan *resource* dari `config/config.json`:
* **MQTT Broker:** `127.0.0.1:1883` (TCP)
* **CoAP Server:** `127.0.0.1:5683` (UDP)
* **Jalur R1 (MQTT → CoAP):**
  `mqtt_pub.py` → publish ke `iot/sensor/mqtt2coap` → `conversion_server.py` (subscribe) → CoAP `POST` ke `actuator/data` → `coap_server.py` → verifikasi oleh `coap_client.py` (mode `GET` ke `actuator/data`).
* **Jalur R2 (CoAP → MQTT):**
  `coap_client.py` (mode `POST` ke `sensor/data`) → `coap_server.py` → `conversion_server.py` (CoAP `GET` polling tiap `1.0` detik + deduplikasi `last_id`) → publish ke `iot/actuator/coap2mqtt` → verifikasi oleh `mqtt_sub.py`.

### 2. Format Payload JSON
Setiap pesan yang dikirim oleh `mqtt_pub.py` maupun `coap_client.py` wajib mengikuti struktur JSON berikut:

```json
{
  "id": "msg-mqtt2coap-0001",
  "sequence": 1,
  "sent_ts": 1732611900.123456,
  "source": "mqtt_publisher",
  "direction": "mqtt_to_coap",
  "payload_size_bytes": 1024,
  "payload": "DATA_UJI_SISTEM_TERDISTRIBUSI_..."
}
```

* `id` *(string)*: Identifier unik per pesan untuk deduplikasi dan perhitungan *conversion loss*.
* `sequence` *(integer)*: Nomor urut pesan (`1, 2, 3, ...`) dalam satu sesi uji.
* `sent_ts` *(float)*: Unix timestamp detik presisi tinggi (`time.time()`) saat pesan dibuat.
* `source` *(string)*: Asal komponen (`"mqtt_publisher"` atau `"coap_post_client"`) untuk mencegah *loop*.
* `direction` *(string)*: Arah skenario (`"mqtt_to_coap"` atau `"coap_to_mqtt"`).
* `payload_size_bytes` *(integer)*: Ukuran aktual payload dalam byte (default `1024` byte sesuai `config.json`).
* `payload` *(string)*: Isi data uji sesuai target ukuran payload.

### 3. Skema File Log CSV (`results/raw_logs/`)
Setiap modul wajib mencatat log dengan nama kolom persis seperti di bawah ini agar dapat langsung diolah oleh `scripts/analyze_results.py`:

* **`sender_log.csv`** (ditulis oleh `mqtt_pub.py` & `coap_client.py` mode `POST`):
  ```csv
  id,sequence,direction,sent_ts,payload_size_bytes
  ```
* **`converter_log.csv`** (ditulis oleh `conversion_server.py`):
  ```csv
  id,direction,receive_ts,convert_done_ts,conversion_delay_ms,status
  ```
  *(Nilai `status`: `FORWARDED`, `SKIPPED_DUPLICATE`, atau `INVALID_JSON`)*
* **`receiver_log.csv`** (ditulis oleh `mqtt_sub.py` & `coap_client.py` mode `GET`):
  ```csv
  id,sequence,direction,sent_ts,final_receive_ts,e2e_delay_ms,integrity_ok
  ```
* **`cpu_log.csv`** (ditulis oleh `monitor_cpu.py`):
  ```csv
  timestamp,direction,run_id,cpu_percent,memory_mb
  ```