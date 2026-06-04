"""
Análisis de métricas de la Tarea 2.

Procesa los CSV generados por el Sistema de Métricas y calcula, por escenario,
las métricas exigidas por el enunciado:

    Throughput        consultas 'processed' por segundo
    Latencia p50/p95  percentiles de latencia (ms) de las consultas resueltas
    Retry rate        % de consultas que pasaron por un tópico de reintento
    Recovery rate     % de consultas recuperadas tras fallar
    DLQ rate          % de consultas enviadas a la Dead Letter Queue
    Backlog           evolución del lag en Kafka (a partir de *_backlog.csv)
    Recovery time     tiempo en volver el backlog a ~0 tras un pico/falla

Uso:
    python data/analisis_kafka.py                # procesa data/resultados/*
    python data/analisis_kafka.py archivo.csv    # procesa un CSV puntual

Salidas: data/resumen_kafka.csv y gráficos PNG en data/graficos/.
"""
import os
import sys
import glob

import pandas as pd

try:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    HAS_PLT = True
except Exception:
    HAS_PLT = False

BASE = os.path.dirname(os.path.abspath(__file__))
RESULTADOS = os.path.join(BASE, "resultados")
GRAFICOS = os.path.join(BASE, "graficos")
os.makedirs(GRAFICOS, exist_ok=True)


# Nombres de columna por formato (según el número de campos).
COLS_BASE = ["query", "zone", "source", "latency", "timestamp"]          # Tarea 1
COLS_KAFKA = ["event_type", "query", "zone", "source", "latency",
              "retry_count", "query_id", "timestamp"]                    # Tarea 2
HEADER_TOKENS = {"event_type", "timestamp", "query"}


def cargar_metrics_csv(path):
    """Carga un CSV de métricas tolerando archivos del sistema base sin cabecera."""
    df = pd.read_csv(path)
    df.columns = df.columns.str.strip()

    # Si la "cabecera" es en realidad una fila de datos (CSV base sin header),
    # ninguno de los tokens conocidos aparecerá -> recargar con header=None.
    if not (set(df.columns) & HEADER_TOKENS):
        ncols = pd.read_csv(path, header=None, nrows=1).shape[1]
        names = COLS_KAFKA if ncols == len(COLS_KAFKA) else COLS_BASE
        if ncols != len(names):  # formato inesperado: no forzamos nombres
            return df
        df = pd.read_csv(path, header=None, names=names)
        df.columns = df.columns.str.strip()
    return df


def analizar_metrics(path):
    df = cargar_metrics_csv(path)
    if df is None or df.empty:
        return None

    # Compatibilidad con el CSV del sistema base (sin event_type).
    if "event_type" not in df.columns:
        df["event_type"] = "processed"
    if "retry_count" not in df.columns:
        df["retry_count"] = 0

    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")

    processed = df[df["event_type"] == "processed"]
    retries = df[df["event_type"] == "retry"]
    recovered = df[df["event_type"] == "recovered"]
    dlq = df[df["event_type"] == "dlq"]

    total_proc = len(processed)
    if total_proc == 0:
        return None

    # Throughput: consultas procesadas / ventana de tiempo observada.
    span = (df["timestamp"].max() - df["timestamp"].min()).total_seconds()
    span = span if span and span > 0 else 1.0
    throughput = total_proc / span

    lat = processed["latency"].astype(float)
    hits = len(processed[processed["source"].astype(str).str.strip() == "cache"])

    return {
        "escenario": os.path.basename(path).replace("_metrics.csv", "").replace(".csv", ""),
        "procesadas": total_proc,
        "duracion_s": round(span, 1),
        "throughput_rps": round(throughput, 2),
        "lat_p50_ms": round(lat.quantile(0.50), 2),
        "lat_p95_ms": round(lat.quantile(0.95), 2),
        "lat_avg_ms": round(lat.mean(), 2),
        "cache_hit_rate_%": round(hits / total_proc * 100, 2),
        "reintentos": len(retries),
        "recuperadas": len(recovered),
        "dlq": len(dlq),
        "retry_rate_%": round(len(retries) / total_proc * 100, 2),
        "recovery_rate_%": round(len(recovered) / len(retries) * 100, 2) if len(retries) else 0.0,
        "dlq_rate_%": round(len(dlq) / total_proc * 100, 2),
    }


def analizar_backlog(path, nombre):
    """Grafica el backlog vs tiempo y estima el recovery time."""
    df = pd.read_csv(path)
    df.columns = df.columns.str.strip()
    if df.empty:
        return None
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    total = df.groupby("timestamp")["lag"].sum().reset_index()

    pico = total["lag"].max()
    # Recovery time: desde el pico de backlog hasta que vuelve a <= 5 % del pico.
    recovery_time = None
    if pico > 0:
        idx_pico = total["lag"].idxmax()
        t_pico = total.loc[idx_pico, "timestamp"]
        post = total.loc[idx_pico:]
        recuperado = post[post["lag"] <= max(1, 0.05 * pico)]
        if not recuperado.empty:
            recovery_time = (recuperado.iloc[0]["timestamp"] - t_pico).total_seconds()

    if HAS_PLT:
        plt.figure(figsize=(9, 4))
        plt.plot(total["timestamp"], total["lag"], color="#c0392b")
        plt.title(f"Backlog (lag total) — {nombre}")
        plt.xlabel("tiempo")
        plt.ylabel("mensajes pendientes")
        plt.tight_layout()
        plt.savefig(os.path.join(GRAFICOS, f"{nombre}_backlog.png"))
        plt.close()

    return {"backlog_pico": int(pico), "recovery_time_s": recovery_time}


def main():
    args = sys.argv[1:]
    if args:
        metric_files = args
    else:
        metric_files = sorted(glob.glob(os.path.join(RESULTADOS, "*_metrics.csv")))
        if not metric_files and os.path.exists(os.path.join(BASE, "metrics_log.csv")):
            metric_files = [os.path.join(BASE, "metrics_log.csv")]

    if not metric_files:
        print("No se encontraron CSV de métricas. Corre primero los escenarios.")
        return

    filas = []
    for mf in metric_files:
        res = analizar_metrics(mf)
        if res is None:
            print(f"Saltando {mf} (vacío)")
            continue

        # Backlog asociado, si existe.
        bf = mf.replace("_metrics.csv", "_backlog.csv")
        if os.path.exists(bf):
            bres = analizar_backlog(bf, res["escenario"])
            if bres:
                res.update(bres)
        filas.append(res)
        print(f"OK: {res['escenario']}")

    if not filas:
        print("No se generaron estadísticas.")
        return

    resumen = pd.DataFrame(filas)
    out = os.path.join(BASE, "resumen_kafka.csv")
    resumen.to_csv(out, index=False)
    print(f"\nResumen guardado en {out}:\n")
    print(resumen.to_string(index=False))

    # Gráfico comparativo de throughput y latencia entre escenarios.
    if HAS_PLT and len(resumen) >= 1:
        fig, axes = plt.subplots(1, 2, figsize=(13, 5))
        axes[0].bar(resumen["escenario"], resumen["throughput_rps"], color="#2980b9")
        axes[0].set_title("Throughput por escenario")
        axes[0].set_ylabel("req/s")
        axes[0].tick_params(axis="x", rotation=45)

        axes[1].bar(resumen["escenario"], resumen["lat_p95_ms"], color="#27ae60")
        axes[1].set_title("Latencia p95 por escenario")
        axes[1].set_ylabel("ms")
        axes[1].tick_params(axis="x", rotation=45)
        plt.tight_layout()
        plt.savefig(os.path.join(GRAFICOS, "comparativa_escenarios.png"))
        plt.close()
        print(f"\nGráficos en {GRAFICOS}/")

    plot_escalado(resumen)


def plot_escalado(resumen):
    """Gráfico del experimento de escalado: métricas vs. nº de consumidores."""
    import re
    sc = resumen[resumen["escenario"].str.match(r"scaling_\d+_consumers")].copy()
    if sc.empty:
        return
    sc["consumidores"] = sc["escenario"].str.extract(r"scaling_(\d+)_consumers").astype(int)
    sc = sc.sort_values("consumidores")

    print("\n=== Escalado de consumidores ===")
    print(sc[["consumidores", "throughput_rps", "lat_p50_ms", "lat_p95_ms",
              "backlog_pico"]].to_string(index=False))

    if not HAS_PLT:
        return
    fig, axes = plt.subplots(1, 3, figsize=(16, 5))
    x = sc["consumidores"]
    axes[0].plot(x, sc["throughput_rps"], "o-", color="#2980b9")
    axes[0].set_title("Throughput vs. consumidores")
    axes[0].set_xlabel("nº consumidores"); axes[0].set_ylabel("req/s")

    axes[1].plot(x, sc["lat_p50_ms"], "o-", label="p50", color="#16a085")
    axes[1].plot(x, sc["lat_p95_ms"], "o-", label="p95", color="#c0392b")
    axes[1].set_title("Latencia vs. consumidores")
    axes[1].set_xlabel("nº consumidores"); axes[1].set_ylabel("ms"); axes[1].legend()

    axes[2].plot(x, sc["backlog_pico"], "o-", color="#8e44ad")
    axes[2].set_title("Backlog pico vs. consumidores")
    axes[2].set_xlabel("nº consumidores"); axes[2].set_ylabel("mensajes")

    for ax in axes:
        ax.set_xticks(x)
        ax.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(os.path.join(GRAFICOS, "escalado_consumidores.png"))
    plt.close()
    print(f"Gráfico de escalado: {GRAFICOS}/escalado_consumidores.png")


if __name__ == "__main__":
    main()
