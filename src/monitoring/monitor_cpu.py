import argparse
import csv
import logging
import time
from pathlib import Path

import psutil

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [MONITOR-CPU] %(levelname)s: %(message)s",
)
logger = logging.getLogger(__name__)

ROOT_DIR = Path(__file__).resolve().parents[2]
CPU_LOG = ROOT_DIR / "results" / "raw_logs" / "cpu_log.csv"
CPU_HEADERS = ["timestamp", "direction", "run_id", "cpu_percent", "memory_mb"]


def ensure_csv(path: Path, headers: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists() or path.stat().st_size == 0:
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(headers)


def find_target_process(target_name: str, pid: int | None) -> psutil.Process | None:
    if pid is not None:
        return psutil.Process(pid) if psutil.pid_exists(pid) else None

    for proc in psutil.process_iter(["pid", "name", "cmdline"]):
        try:
            cmdline = " ".join(proc.info.get("cmdline") or [])
            if target_name in cmdline and "monitor_cpu.py" not in cmdline:
                return proc
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue
    return None


def monitor(
    target_name: str,
    pid: int | None,
    direction: str,
    run_id: str,
    interval: float,
    duration: float | None,
) -> None:
    ensure_csv(CPU_LOG, CPU_HEADERS)

    logger.info("Mencari proses target (%s / pid=%s)...", target_name, pid)
    proc = None
    while proc is None:
        proc = find_target_process(target_name, pid)
        if proc is None:
            time.sleep(0.5)

    logger.info(
        "Memantau PID=%d | direction=%s | run_id=%s | interval=%.1fs",
        proc.pid,
        direction,
        run_id,
        interval,
    )

    # Panggilan awal untuk inisialisasi penghitung CPU psutil
    proc.cpu_percent(interval=None)
    start_time = time.monotonic()

    try:
        while True:
            if duration is not None and (time.monotonic() - start_time) >= duration:
                break

            cpu_pct = proc.cpu_percent(interval=interval)
            mem_mb = proc.memory_info().rss / (1024 * 1024)
            ts = time.time()

            with open(CPU_LOG, "a", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow([f"{ts:.6f}", direction, run_id, f"{cpu_pct:.2f}", f"{mem_mb:.2f}"])

            logger.info("CPU=%.2f%% | RAM=%.2f MB", cpu_pct, mem_mb)
    except psutil.NoSuchProcess:
        logger.warning("Proses target (PID=%d) telah berhenti.", proc.pid)
    except KeyboardInterrupt:
        logger.info("Monitoring dihentikan oleh pengguna.")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Monitor CPU & RAM Conversion Server")
    parser.add_argument("--target", type=str, default="conversion_server.py", help="Nama file proses target")
    parser.add_argument("--pid", type=int, default=None, help="PID proses target (opsional)")
    parser.add_argument("--direction", choices=["mqtt_to_coap", "coap_to_mqtt"], required=True)
    parser.add_argument("--run-id", type=str, default="run1")
    parser.add_argument("--interval", type=float, default=1.0, help="Interval sampling (detik)")
    parser.add_argument("--duration", type=float, default=None, help="Durasi monitoring (detik)")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    monitor(args.target, args.pid, args.direction, args.run_id, args.interval, args.duration)