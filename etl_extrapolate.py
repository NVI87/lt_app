#!/usr/bin/env python3
"""
Экстраполяция статистики времени и памяти обработки по размеру файла
на основе CSV, полученного из etl_file_stats.py (только DONE-записи).

Для каждой пары (attach_type, stage) строится линейная регрессия
  wall_time_sec  ~ a_t * size_mb + b_t
  rss_peak_bytes ~ a_m * size_mb + b_m
по фактическим DONE-наблюдениям (метод наименьших квадратов, numpy.polyfit).
Также считается R^2, чтобы видеть, насколько модели можно доверять.

Дополнительно считается суммарная модель по документу (все стадии вместе):
  total_wall_time_sec ~ f(size_mb)  -- на основе суммы предсказаний по стадиям.

На выходе:
  - CSV с коэффициентами регрессии по (attach_type, stage): coefficients.csv
  - CSV с прогнозом time/memory для набора целевых размеров: projection.csv

Использование:
    python etl_extrapolate.py etl_stats_done.csv --out-dir out/
    python etl_extrapolate.py etl_stats_done.csv --sizes 10 50 100 250 500 1000
"""

import argparse
import os

import numpy as np
import pandas as pd

BYTES_IN_MB = 1024 * 1024
DEFAULT_TARGET_SIZES_MB = [1, 5, 10, 25, 50, 100, 200, 500, 1000]


def load_done(path):
    df = pd.read_csv(path)
    df = df[df["stage"].notna() & df["wall_time_sec"].notna()].copy()
    df["size_mb"] = df["attach_size"] / BYTES_IN_MB
    return df


def fit_linear(x, y):
    """Линейная регрессия y ~ a*x + b, возвращает (a, b, r2). Если точек < 2 — None."""
    x = np.asarray(x, dtype=float)
    y = np.asarray(y, dtype=float)
    mask = np.isfinite(x) & np.isfinite(y)
    x, y = x[mask], y[mask]
    if len(x) < 2 or np.all(x == x[0]):
        # Недостаточно точек или нет разброса по размеру - строим прямую через ноль
        # по единственной наблюдаемой точке (грубая оценка "k = y/x").
        if len(x) >= 1 and x[0] != 0:
            a = y[0] / x[0]
            return a, 0.0, None
        return None
    a, b = np.polyfit(x, y, 1)
    y_pred = a * x + b
    ss_res = np.sum((y - y_pred) ** 2)
    ss_tot = np.sum((y - y.mean()) ** 2)
    r2 = 1 - ss_res / ss_tot if ss_tot > 0 else None
    return a, b, r2


def build_models(df):
    """Строит регрессии time и memory для каждой (attach_type, stage)."""
    rows = []
    for (att_type, stage), g in df.groupby(["attach_type", "stage"]):
        n = len(g)
        time_fit = fit_linear(g["size_mb"], g["wall_time_sec"])
        mem_fit = fit_linear(g["size_mb"], g["rss_peak"]) if "rss_peak" in g else None

        a_t, b_t, r2_t = time_fit if time_fit else (None, None, None)
        a_m, b_m, r2_m = mem_fit if mem_fit else (None, None, None)

        rows.append({
            "attach_type": att_type,
            "stage": stage,
            "n_samples": n,
            "size_mb_min": g["size_mb"].min(),
            "size_mb_max": g["size_mb"].max(),
            "time_slope_sec_per_mb": a_t,
            "time_intercept_sec": b_t,
            "time_r2": r2_t,
            "mem_slope_bytes_per_mb": a_m,
            "mem_intercept_bytes": b_m,
            "mem_r2": r2_m,
        })
    return pd.DataFrame(rows)


def project(models_df, target_sizes_mb):
    """Прогноз time/memory для целевых размеров по каждой (attach_type, stage)."""
    rows = []
    for _, m in models_df.iterrows():
        for size_mb in target_sizes_mb:
            pred_time = None
            if pd.notna(m["time_slope_sec_per_mb"]):
                pred_time = max(0.0, m["time_slope_sec_per_mb"] * size_mb + (m["time_intercept_sec"] or 0.0))
            pred_mem_mb = None
            if pd.notna(m["mem_slope_bytes_per_mb"]):
                pred_mem_bytes = max(0.0, m["mem_slope_bytes_per_mb"] * size_mb + (m["mem_intercept_bytes"] or 0.0))
                pred_mem_mb = pred_mem_bytes / BYTES_IN_MB
            rows.append({
                "attach_type": m["attach_type"],
                "stage": m["stage"],
                "n_samples_source": m["n_samples"],
                "observed_range_mb": f"{m['size_mb_min']:.1f}-{m['size_mb_max']:.1f}",
                "target_size_mb": size_mb,
                "extrapolated": size_mb > m["size_mb_max"] or size_mb < m["size_mb_min"],
                "predicted_wall_time_sec": round(pred_time, 2) if pred_time is not None else None,
                "predicted_rss_peak_mb": round(pred_mem_mb, 2) if pred_mem_mb is not None else None,
                "time_r2": round(m["time_r2"], 3) if pd.notna(m["time_r2"]) else None,
            })
            return pd.DataFrame(rows)

def total_by_document(proj_df):
    """Суммарное время/пик памяти по всем стадиям для каждого (type, target_size)."""
    return (
        proj_df.groupby(["attach_type", "target_size_mb"])
        .agg(
            total_wall_time_sec=("predicted_wall_time_sec", "sum"),
            max_rss_peak_mb=("predicted_rss_peak_mb", "max"),
            extrapolated_any=("extrapolated", "any"),
        )
        .reset_index()
    )

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("done_csv", help="CSV со статистикой DONE-стадий (etl_stats_done.csv)")
    parser.add_argument("--out-dir", default=".", help="Директория для сохранения результатов")
    parser.add_argument("--sizes", nargs="*", type=float, default=DEFAULT_TARGET_SIZES_MB,
                        help="Целевые размеры файлов в МБ для прогноза")
    args = parser.parse_args()

    os.makedirs(args.out_dir, exist_ok=True)

    df = load_done(args.done_csv)
    models = build_models(df)
    proj = project(models, args.sizes)
    totals = total_by_document(proj)

    models.to_csv(os.path.join(args.out_dir, "coefficients.csv"), index=False)
    proj.to_csv(os.path.join(args.out_dir, "projection_by_stage.csv"), index=False)
    totals.to_csv(os.path.join(args.out_dir, "projection_total.csv"), index=False)

    print(f"Коэффициенты регрессии по стадиям сохранены: coefficients.csv ({len(models)} строк)")
    print(f"Прогноз по стадиям сохранён: projection_by_stage.csv ({len(proj)} строк)")
    print(f"Суммарный прогноз по документу сохранён: projection_total.csv ({len(totals)} строк)")
    print("\n=== Коэффициенты (сек/МБ и МБ памяти/МБ файла) ===")
    print(models[["attach_type", "stage", "n_samples", "time_slope_sec_per_mb", "time_r2"]]
          .round(3).to_string(index=False))
    print("\n=== Суммарный прогноз по документу ===")
    print(totals.round(1).to_string(index=False))

if __name__ == "__main__":
    main()
