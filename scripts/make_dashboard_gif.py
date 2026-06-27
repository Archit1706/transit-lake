"""Generate an animated preview GIF of the dashboard from the live marts.

Renders the key analyses (metrics, worst OTP, live fleet map, congestion by hour,
delay vs congestion) as dark-themed frames and assembles them into docs/dashboard.gif.
Browser-free — uses matplotlib + Pillow.

Run:  uv run python -m scripts.make_dashboard_gif
"""
import io
from pathlib import Path

import duckdb
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from PIL import Image

REPO = Path(__file__).resolve().parent.parent
DB = REPO / "lake" / "transitlake.duckdb"
OUT = REPO / "docs" / "dashboard.gif"

BG = "#0E1117"
FG = "#FAFAFA"
MUTED = "#9AA0A6"
BUS = "#4FC3F7"
TRAIN = "#FF7043"
ACCENT = "#66BB6A"

plt.rcParams.update({
    "figure.facecolor": BG, "axes.facecolor": BG, "savefig.facecolor": BG,
    "text.color": FG, "axes.labelcolor": FG, "xtick.color": MUTED,
    "ytick.color": MUTED, "axes.edgecolor": "#2A2E37", "font.size": 12,
})

con = duckdb.connect(str(DB), read_only=True)


def fig_to_frame(fig) -> Image.Image:
    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=100, bbox_inches="tight", facecolor=BG)
    plt.close(fig)
    buf.seek(0)
    return Image.open(buf).convert("RGB")


def _new():
    fig, ax = plt.subplots(figsize=(10, 6))
    return fig, ax


frames = []

# Frame 1 — cover + metrics
fresh = {r[0]: r for r in con.execute("select source, latest_ts, row_count from main_marts.mart_pipeline_freshness").fetchall()}
fig, ax = _new()
ax.axis("off")
ax.text(0.5, 0.86, "TransitLake", ha="center", fontsize=38, weight="bold", color=FG)
ax.text(0.5, 0.74, "A Dagster-orchestrated medallion lakehouse for Chicago transit",
        ha="center", fontsize=14, color=MUTED)
cards = [
    (f"{int(fresh['vehicle_positions'][2]):,}", "real-time positions"),
    (f"{int(fresh['congestion'][2]):,}", "congestion rows"),
    ("36", "dbt models"),
    ("100+", "data-quality checks"),
]
for i, (big, small) in enumerate(cards):
    x = 0.13 + i * 0.25
    ax.text(x, 0.45, big, ha="center", fontsize=26, weight="bold", color=ACCENT)
    ax.text(x, 0.36, small, ha="center", fontsize=12, color=MUTED)
ax.text(0.5, 0.15, "bronze → silver → gold   ·   CTA bus + rail · congestion · weather",
        ha="center", fontsize=12, color=MUTED)
frames.append(fig_to_frame(fig))

# Frame 2 — worst on-time performance by route
otp = con.execute("""
    select route_name, on_time_rate from main_marts.mart_otp_by_route
    where total_reports >= 20 order by on_time_rate asc limit 12
""").fetchall()
if otp:
    names = [r[0][:22] for r in otp][::-1]
    rates = [r[1] * 100 for r in otp][::-1]
    fig, ax = _new()
    ax.barh(names, rates, color=BUS)
    ax.set_title("Worst on-time performance by route", color=FG, fontsize=16, weight="bold", loc="left")
    ax.set_xlabel("On-time rate (%)")
    ax.set_xlim(min(rates) - 2, 100)
    for s in ("top", "right"):
        ax.spines[s].set_visible(False)
    frames.append(fig_to_frame(fig))

# Frame 3 — live fleet map
fleet = con.execute("""
    with latest as (select max(report_ts) m from main_marts.fact_vehicle_position)
    select mode, lat, lon from main_marts.fact_vehicle_position, latest
    where report_ts >= latest.m - interval 30 minute
""").fetchall()
if fleet:
    fig, ax = _new()
    for mode, color in (("bus", BUS), ("train", TRAIN)):
        pts = [(r[2], r[1]) for r in fleet if r[0] == mode]
        if pts:
            xs, ys = zip(*pts)
            ax.scatter(xs, ys, s=6, c=color, alpha=0.7, label=f"{mode} ({len(pts)})")
    ax.set_title("Live fleet — recent CTA vehicle positions", color=FG, fontsize=16, weight="bold", loc="left")
    ax.set_aspect("equal")
    ax.set_xticks([]); ax.set_yticks([])
    leg = ax.legend(loc="upper right", facecolor=BG, edgecolor="#2A2E37", labelcolor=FG)
    frames.append(fig_to_frame(fig))

# Frame 4 — city-wide road speed by hour
cbh = con.execute("select report_hour, avg_speed_mph from main_marts.mart_congestion_by_hour order by report_hour").fetchall()
if cbh:
    hrs = [r[0] for r in cbh]
    spd = [r[1] for r in cbh]
    fig, ax = _new()
    ax.plot(hrs, spd, color=ACCENT, marker="o", linewidth=2)
    ax.fill_between(hrs, spd, min(spd) - 1, color=ACCENT, alpha=0.12)
    ax.set_title("City-wide average road speed by hour", color=FG, fontsize=16, weight="bold", loc="left")
    ax.set_xlabel("Hour of day"); ax.set_ylabel("Avg speed (mph)")
    for s in ("top", "right"):
        ax.spines[s].set_visible(False)
    frames.append(fig_to_frame(fig))

con.close()

# Normalise frame sizes and write the GIF (hold each frame ~2s)
w = min(f.width for f in frames)
h = min(f.height for f in frames)
frames = [f.resize((w, h)) for f in frames]
OUT.parent.mkdir(parents=True, exist_ok=True)
frames[0].save(OUT, save_all=True, append_images=frames[1:], duration=2200, loop=0, optimize=True)
print(f"wrote {OUT} ({len(frames)} frames, {OUT.stat().st_size // 1024} KB)")
