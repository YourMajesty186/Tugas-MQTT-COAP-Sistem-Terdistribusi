from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

ROOT_DIR = Path(__file__).resolve().parents[1]
RAW_DIR = ROOT_DIR / "results" / "raw_logs"
SUMMARY_DIR = ROOT_DIR / "results" / "summary"
CHARTS_DIR = ROOT_DIR / "results" / "charts"


def safe_read_csv(path: Path) -> pd.DataFrame:
    if not path.exists() or path.stat().st_size == 0:
        return pd.DataFrame()
    return pd.read_csv(path)


def add_minute_bucket(df: pd.DataFrame, ts_col: str) -> pd.DataFrame:
    df = df.copy()
    df[ts_col] = pd.to_numeric(df[ts_col], errors="coerce")
    df = df.dropna(subset=[ts_col])
    df["minute"] = (
        df.groupby("direction")[ts_col]
        .transform(lambda s: ((s - s.min()) // 60).astype(int) + 1)
    )
    return df


def build_summary(
    sender_df: pd.DataFrame,
    converter_df: pd.DataFrame,
    receiver_df: pd.DataFrame,
    cpu_df: pd.DataFrame,
) -> pd.DataFrame:
    directions = sorted(
        set(sender_df.get("direction", pd.Series(dtype=str)).dropna().unique())
        | set(receiver_df.get("direction", pd.Series(dtype=str)).dropna().unique())
        | set(cpu_df.get("direction", pd.Series(dtype=str)).dropna().unique())
    )

    rows = []
    for d in directions:
        s_dir = sender_df[sender_df["direction"] == d] if not sender_df.empty else pd.DataFrame()
        c_dir = (
            converter_df[
                (converter_df["direction"] == d) & (converter_df["status"] == "FORWARDED")
            ]
            if not converter_df.empty
            else pd.DataFrame()
        )
        r_dir = receiver_df[receiver_df["direction"] == d] if not receiver_df.empty else pd.DataFrame()
        u_dir = cpu_df[cpu_df["direction"] == d] if not cpu_df.empty else pd.DataFrame()

        sent_ids = set(s_dir["id"].astype(str)) if not s_dir.empty else set()
        recv_ids = set(r_dir["id"].astype(str)) if not r_dir.empty else set()
        matched_ids = sent_ids & recv_ids if sent_ids else recv_ids

        n_sent = len(sent_ids)
        n_recv = len(matched_ids)
        success_pct = (n_recv / n_sent * 100.0) if n_sent > 0 else 0.0
        loss_pct = ((n_sent - n_recv) / n_sent * 100.0) if n_sent > 0 else 0.0

        rows.append(
            {
                "direction": d,
                "n_sent": n_sent,
                "n_received": n_recv,
                "conversion_success_pct": round(success_pct, 2),
                "conversion_loss_pct": round(loss_pct, 2),
                "avg_conversion_delay_ms": round(c_dir["conversion_delay_ms"].mean(), 3)
                if not c_dir.empty
                else None,
                "avg_e2e_delay_ms": round(r_dir["e2e_delay_ms"].mean(), 3)
                if not r_dir.empty
                else None,
                "cpu_mean_pct": round(u_dir["cpu_percent"].mean(), 2) if not u_dir.empty else None,
                "cpu_peak_pct": round(u_dir["cpu_percent"].max(), 2) if not u_dir.empty else None,
                "cpu_p95_pct": round(u_dir["cpu_percent"].quantile(0.95), 2)
                if not u_dir.empty
                else None,
                "memory_mean_mb": round(u_dir["memory_mb"].mean(), 2) if not u_dir.empty else None,
                "memory_peak_mb": round(u_dir["memory_mb"].max(), 2) if not u_dir.empty else None,
                "memory_p95_mb": round(u_dir["memory_mb"].quantile(0.95), 2)
                if not u_dir.empty
                else None,
            }
        )

    return pd.DataFrame(rows)


def plot_metric_per_minute(
    df: pd.DataFrame,
    ts_col: str,
    val_col: str,
    title: str,
    ylabel: str,
    output_path: Path,
) -> None:
    if df.empty:
        print(f"[SKIP] Data kosong untuk grafik: {output_path.name}")
        return

    bucketed = add_minute_bucket(df, ts_col)
    grouped = bucketed.groupby(["direction", "minute"])[val_col].mean().reset_index()

    plt.figure(figsize=(8, 4.5))
    for direction, sub in grouped.groupby("direction"):
        plt.plot(sub["minute"], sub[val_col], marker="o", linewidth=2, label=direction)

    plt.title(title)
    plt.xlabel("Menit Pengujian")
    plt.ylabel(ylabel)
    plt.grid(True, linestyle="--", alpha=0.5)
    plt.legend()
    plt.tight_layout()
    plt.savefig(output_path, dpi=150)
    plt.close()
    print(f"[OK] Grafik disimpan: {output_path}")


def main() -> None:
    SUMMARY_DIR.mkdir(parents=True, exist_ok=True)
    CHARTS_DIR.mkdir(parents=True, exist_ok=True)

    sender_df = safe_read_csv(RAW_DIR / "sender_log.csv")
    converter_df = safe_read_csv(RAW_DIR / "converter_log.csv")
    receiver_df = safe_read_csv(RAW_DIR / "receiver_log.csv")
    cpu_df = safe_read_csv(RAW_DIR / "cpu_log.csv")

    summary_df = build_summary(sender_df, converter_df, receiver_df, cpu_df)
    summary_path = SUMMARY_DIR / "summary_metrics.csv"
    summary_df.to_csv(summary_path, index=False)
    print(f"[OK] Ringkasan metrik disimpan: {summary_path}")
    if not summary_df.empty:
        print(summary_df.to_string(index=False))

    # 1. Grafik rerata end-to-end delay per menit
    plot_metric_per_minute(
        receiver_df,
        ts_col="final_receive_ts",
        val_col="e2e_delay_ms",
        title="Rerata Delay Ujung ke Ujung per Menit",
        ylabel="End-to-End Delay (ms)",
        output_path=CHARTS_DIR / "e2e_delay_per_minute.png",
    )

    # 2. Grafik rerata conversion delay per menit
    forwarded_df = (
        converter_df[converter_df["status"] == "FORWARDED"]
        if not converter_df.empty
        else pd.DataFrame()
    )
    plot_metric_per_minute(
        forwarded_df,
        ts_col="receive_ts",
        val_col="conversion_delay_ms",
        title="Rerata Delay Konversi per Menit",
        ylabel="Conversion Delay (ms)",
        output_path=CHARTS_DIR / "conversion_delay_per_minute.png",
    )

    # 3. Grafik rerata penggunaan CPU conversion server per menit
    plot_metric_per_minute(
        cpu_df,
        ts_col="timestamp",
        val_col="cpu_percent",
        title="Rerata Penggunaan CPU Conversion Server per Menit",
        ylabel="CPU Usage (%)",
        output_path=CHARTS_DIR / "cpu_usage_per_minute.png",
    )


if __name__ == "__main__":
    main()