'''
FILE: analyze_blocked_crossings.py
PURPOSE: Joins public railroad blockage reports with the official FRA Form 71 
         crossing inventory. Analyzes national data sparseness vs. local hotspot 
         density. Compares blocked vs. unblocked crossings, and exports summary 
         CSVs and charts.
    
'''

from __future__ import annotations

import argparse
from pathlib import Path

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt
import pandas as pd
'''
import seaborn as sns
'''

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_BLOCKED_XLSX = REPO_ROOT / "data" / "blocked_crossings_2025.xlsx"
DEFAULT_BLOCKED_CSV = REPO_ROOT / "data" / "blocked_crossings_2025(Sheet1).csv"
DEFAULT_INVENTORY = REPO_ROOT / "data" / "Crossing_Inventory_Data_(Form_71)_-_Current_20260707.csv"
DEFAULT_GXAPS_DIR = REPO_ROOT / "data"
DEFAULT_OUTPUT = REPO_ROOT / "analysis_outputs"
DEFAULT_DISCARDED_GXAPS = DEFAULT_OUTPUT / "discarded_gxaps_records.csv"
GXAPS_FILE_PATTERN = "GXAPS_*.xlsx"
GXAPS_SHEET_NAME = "Annual Report"
GXAPS_DATA_START_ROW = 6


def read_csv(path: Path, **kwargs: object) -> pd.DataFrame:
    return pd.read_csv(path, encoding="utf-8", encoding_errors="replace", **kwargs)


def clean_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = df.columns.str.strip()
    return df


def clean_ids(series: pd.Series) -> pd.Series:
    return series.astype("string").str.strip()


def to_numeric(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series.astype("string").str.replace(",", "", regex=False), errors="coerce")


def summarize_counts(counts: pd.Series) -> pd.DataFrame:
    stats = counts.describe(percentiles=[0.5, 0.75, 0.9, 0.95, 0.99]).rename("value")
    return stats.reset_index().rename(columns={"index": "metric"})


def save_bar(series: pd.Series, title: str, xlabel: str, ylabel: str, path: Path, rotation: int = 45) -> None:
    fig, ax = plt.subplots(figsize=(11, 6))
    series.plot(kind="bar", ax=ax, color="#2c7fb8", width=0.85)
    ax.set_title(title)
    ax.set_xlabel(xlabel)
    ax.set_ylabel(ylabel)
    ax.tick_params(axis="x", rotation=rotation)
    plt.tight_layout()
    fig.savefig(path, dpi=180)
    plt.close(fig)


def save_histogram(counts: pd.Series, path: Path) -> None:
    fig, ax = plt.subplots(figsize=(10, 6))
    max_count = int(counts.max())
    if max_count <= 1:
        bins = [0.5, 1.5]
    else:
        bins = [0.5, 1.5, 2.5, 3.5, 5.5, 8.5, 13.5, 21.5, 34.5, 55.5, 89.5, 144.5, 233.5, 377.5, 610.5, max_count + 0.5]
    ax.hist(counts, bins=bins, color="#f03b20", edgecolor="white")
    ax.set_xscale("log")
    ax.set_title("Blocked events per crossing")
    ax.set_xlabel("Blocked event count per crossing (log scale)")
    ax.set_ylabel("Number of crossings")
    plt.tight_layout()
    fig.savefig(path, dpi=180)
    plt.close(fig)


def save_group_boxplot(
    blocked_values: pd.Series,
    unblocked_values: pd.Series,
    title: str,
    ylabel: str,
    path: Path,
    log_scale: bool = False,
) -> None:
    fig, ax = plt.subplots(figsize=(9, 6))
    ax.boxplot(
        [blocked_values.dropna(), unblocked_values.dropna()],
        tick_labels=["Blocked", "Unblocked"],
        showfliers=False,
        patch_artist=True,
        boxprops={"facecolor": "#2c7fb8", "alpha": 0.7},
        medianprops={"color": "black"},
    )
    if log_scale:
        ax.set_yscale("log")
    ax.set_title(title)
    ax.set_ylabel(ylabel)
    plt.tight_layout()
    fig.savefig(path, dpi=180)
    plt.close(fig)


def make_comparison_table(df: pd.DataFrame, flag_col: str, columns: list[str]) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for column in columns:
        if column not in df.columns:
            continue
        numeric = to_numeric(df[column])
        for is_blocked, subset in ((True, df[df[flag_col]]), (False, df[~df[flag_col]])):
            values = numeric.loc[subset.index].dropna()
            rows.append(
                {
                    "feature": column,
                    "group": "blocked" if is_blocked else "unblocked",
                    "n": int(values.shape[0]),
                    "mean": float(values.mean()) if not values.empty else None,
                    "median": float(values.median()) if not values.empty else None,
                    "p25": float(values.quantile(0.25)) if not values.empty else None,
                    "p75": float(values.quantile(0.75)) if not values.empty else None,
                }
            )
    return pd.DataFrame(rows).sort_values(["feature", "group"]).reset_index(drop=True)


def summarize_state_density(df: pd.DataFrame) -> pd.DataFrame:
    grouped = df.groupby("State Name", dropna=False).agg(
        total_crossings=("Crossing ID", "count"),
        blocked_crossings=("blocked_event_count", lambda s: int((s > 0).sum())),
        blocked_events=("blocked_event_count", "sum"),
    )
    grouped["blocked_crossing_share_pct"] = grouped["blocked_crossings"] / grouped["total_crossings"] * 100
    grouped["blocked_events_per_blocked_crossing"] = grouped["blocked_events"] / grouped["blocked_crossings"].where(
        grouped["blocked_crossings"] > 0
    )
    return grouped.sort_values(["blocked_crossing_share_pct", "blocked_events"], ascending=False).reset_index()


def preprocess_blocked_crossings(blocked_xlsx: Path, blocked_csv: Path) -> pd.DataFrame:
    blocked = clean_columns(pd.read_excel(blocked_xlsx)) # if this were a csv read, would use parse_dates argument
    blocked.to_csv(blocked_csv, index=False)
    if "Date/Time" not in blocked.columns:
        raise KeyError("Missing expected blocked crossing date/time column: Date/Time")
    if not pd.api.types.is_datetime64_any_dtype(blocked["Date/Time"]):
        raise TypeError("Blocked crossing Date/Time column was not read as a pandas datetime dtype")
    return blocked


def format_gxaps_header_value(value: object) -> str:
    # Normalize workbook header values so numeric subheaders become stable column names.
    if pd.isna(value):
        return ""
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    if isinstance(value, int):
        return str(value)
    return str(value).strip()


def merge_gxaps_headers(major_headers: pd.Series, sub_headers: pd.Series) -> tuple[list[str], list[bool]]:
    # Build human-readable column names from the merged major and sub header rows.
    columns: list[str] = []
    keep_mask: list[bool] = []
    current_major = ""
    for major_header, sub_header in zip(major_headers, sub_headers, strict=False):
        major = format_gxaps_header_value(major_header)
        sub = format_gxaps_header_value(sub_header)
        if not major and not sub:
            keep_mask.append(False)
            continue
        keep_mask.append(True)
        if major:
            current_major = major
        if sub.isdigit() and current_major:
            columns.append(f"{current_major} {sub}")
        elif major and sub:
            columns.append(f"{major} {sub}")
        else:
            columns.append(major or sub)
    return columns, keep_mask


def read_gxaps_annual_report(gxaps_path: Path) -> pd.DataFrame:
    # Read the annual report with raw rows so the header structure can be reconstructed explicitly.
    raw = pd.read_excel(gxaps_path, sheet_name=GXAPS_SHEET_NAME, header=None)
    if not all(isinstance(column, int) for column in raw.columns):
        return clean_columns(raw)

    # Split the workbook header rows from the data rows and populate all columns.
    if len(raw.index) < GXAPS_DATA_START_ROW:
        return clean_columns(raw)
    header_rows = raw.iloc[:GXAPS_DATA_START_ROW].copy()
    data = raw.iloc[GXAPS_DATA_START_ROW:].copy()
    columns, keep_mask = merge_gxaps_headers(header_rows.iloc[4], header_rows.iloc[5])
    data = data.loc[:, keep_mask]
    data.columns = columns
    return clean_columns(data)


def preprocess_gxaps(gxaps_dir: Path, output_dir: Path = DEFAULT_OUTPUT) -> pd.DataFrame:
    # Collect every GXAPS workbook before loading so we can fail fast on missing input.
    gxaps_paths = sorted(gxaps_dir.glob(GXAPS_FILE_PATTERN))
    if not gxaps_paths:
        raise FileNotFoundError(f"No GXAPS workbooks found in {gxaps_dir}")

    # Read the annual report sheet from each workbook and normalize the shared key column.
    aps_frames: list[pd.DataFrame] = []
    for gxaps_path in gxaps_paths:
        frame = read_gxaps_annual_report(gxaps_path)
        if "Crossing ID" not in frame.columns:
            raise KeyError(f"Missing expected GXAPS crossing ID column in {gxaps_path.name}: Crossing ID")
        frame["Crossing ID"] = clean_ids(frame["Crossing ID"])
        aps_frames.append(frame)

    # Combine all GXAPS workbooks into one dataframe for later use as aps.
    aps = pd.concat(aps_frames, ignore_index=True, sort=False)

    # Drop workbook total rows before key validation so the SQL-style constraint only sees real crossings.
    aps = aps.loc[aps["Crossing ID"].notna() & aps["Crossing ID"].astype("string").str.strip().ne("")].copy()

    # Split duplicate crossings into a discard frame and a unique frame for analysis.
    duplicate_mask = aps["Crossing ID"].duplicated(keep=False)
    discarded_gxaps_records = aps.loc[duplicate_mask].copy()
    aps = aps.loc[~duplicate_mask].copy()

    # Write discarded duplicate records so the analysis can continue with unique crossing IDs.
    output_dir.mkdir(parents=True, exist_ok=True)
    discarded_gxaps_records.to_csv(output_dir / DEFAULT_DISCARDED_GXAPS.name, index=False)

    return aps


def main() -> None:
    parser = argparse.ArgumentParser(description="Join blocked crossing events to Form 71 inventory data.")
    parser.add_argument("--inventory", type=Path, default=DEFAULT_INVENTORY)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args()

    output_dir = args.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)

    # Load the GXAPS workbooks into aps without joining them to any other frame yet.
    #aps = preprocess_gxaps(DEFAULT_GXAPS_DIR, DEFAULT_OUTPUT)
    # Inspect the deduplicated GXAPS frame and confirm the primary key is unique.
    #print(aps.head())
    #print(pd.DataFrame([('aps_rows', len(aps)), ('aps_crossing_ids', aps['Crossing ID'].nunique())], columns=['metric', 'value']))

    blocked = preprocess_blocked_crossings(DEFAULT_BLOCKED_XLSX, DEFAULT_BLOCKED_CSV)
    inventory = clean_columns(read_csv(args.inventory, low_memory=False))

    blocked["Crossing ID"] = clean_ids(blocked["Crossing ID"])
    inventory["Crossing ID"] = clean_ids(inventory["Crossing ID"])

    inventory["Revision Date"] = pd.to_datetime(inventory.get("Revision Date"), errors="coerce")
    inventory = inventory.sort_values(["Crossing ID", "Revision Date"]).drop_duplicates("Crossing ID", keep="last")

    blocked = blocked.dropna(subset=["Crossing ID"]).copy()
    blocked["Year"] = blocked["Date/Time"].dt.year     

    blocked_counts = blocked.groupby("Crossing ID").size().rename("blocked_event_count").reset_index()
    inventory_with_counts = inventory.merge(blocked_counts, on="Crossing ID", how="left")
    inventory_with_counts["blocked_event_count"] = inventory_with_counts["blocked_event_count"].fillna(0).astype(int)

    blocked_joined = blocked.merge(inventory, left_on=['Crossing ID', 'Year'], right_on=['Crossing ID', 'Trains Per Week Captured Year'], how='left', suffixes=('_blocked', '_inventory'))
    blocked_joined.to_csv(output_dir / 'blocked_events_joined_to_inventory.csv', index=False)
    
    blocked_analysis = blocked_joined[blocked_joined["Street_inventory"].isna()==False]     
    blocked_analysis.to_csv(output_dir / 'blocked_events_joined_to_inventory_WithNonzeroTrainActivity.csv', index=False)

    blocked_inventory = inventory_with_counts[inventory_with_counts["blocked_event_count"] > 0].copy()
    inventory_with_counts["is_blocked"] = inventory_with_counts["blocked_event_count"] > 0
    counts = blocked_inventory["blocked_event_count"]
    state_density = summarize_state_density(inventory_with_counts)
    comparison_features = [
        "Annual Average Daily Traffic Count",
        "Annual Average Daily Traffic Year",
        "Total Daylight Thru Trains",
        "Total Nighttime Thru Trains",
        "Total Switching Trains",
        "Total Transit Trains",
        "Number Of Main Tracks",
        "Number Of Siding Tracks",
        "Number Of Yard Tracks",
        "Maximum Timetable Speed",
        "Highway Speed Limit",
    ]
    comparison_table = make_comparison_table(inventory_with_counts, "is_blocked", comparison_features)
    comparison_table.to_csv(output_dir / "blocked_vs_unblocked_numeric_comparison.csv", index=False)
    state_density.to_csv(output_dir / "state_density_summary.csv", index=False)

    summary_rows = [
        ("blocked_events_total", len(blocked)),
        ("blocked_crossing_ids", blocked["Crossing ID"].nunique()),
        ("inventory_crossings_total", len(inventory)),
        ("inventory_crossings_with_blocked_events", len(blocked_inventory)),
        (
            "inventory_share_with_blocked_events_pct",
            round(len(blocked_inventory) / len(inventory) * 100, 4) if len(inventory) else 0,
        ),
        ("blocked_events_per_blocked_crossing_mean", round(counts.mean(), 4)),
        ("blocked_events_per_blocked_crossing_median", float(counts.median())),
        ("blocked_events_per_blocked_crossing_p95", float(counts.quantile(0.95))),
        ("blocked_events_per_blocked_crossing_p99", float(counts.quantile(0.99))),
        ("blocked_events_per_blocked_crossing_max", int(counts.max())),
    ]
    pd.DataFrame(summary_rows, columns=["metric", "value"]).to_csv(output_dir / "summary_metrics.csv", index=False)

    crossing_summary = blocked_inventory[
        [
            "Crossing ID",
            "blocked_event_count",
            "State Name",
            "County Name",
            "City Name",
            "Railroad Name",
            "Crossing Type",
            "Crossing Purpose",
            "Crossing Position",
            "Public Access",
            "Annual Average Daily Traffic Count",
            "Annual Average Daily Traffic Year",
            "Total Daylight Thru Trains",
            "Total Nighttime Thru Trains",
            "Total Switching Trains",
            "Total Transit Trains",
            "Number Of Main Tracks",
            "Number Of Siding Tracks",
            "Number Of Yard Tracks",
            "Track Signaled",
            "Signs Or Signals",
            "Gate Configuration",
            "Warning Device Code",
            "Latitude",
            "Longitude",
        ]
    ].copy()
    crossing_summary.to_csv(output_dir / "blocked_crossings_summary.csv", index=False)

    reason_counts = blocked["Reason"].value_counts(dropna=False)
    duration_counts = blocked["Duration"].value_counts(dropna=False)
    state_counts = blocked["State"].value_counts(dropna=False)
    top_crossings = counts.sort_values(ascending=False).head(20)

    reason_counts.to_csv(output_dir / "reason_counts.csv", header=["count"])
    duration_counts.to_csv(output_dir / "duration_counts.csv", header=["count"])
    state_counts.to_csv(output_dir / "state_counts.csv", header=["count"])
    summarize_counts(counts).to_csv(output_dir / "blocked_count_distribution.csv", index=False)

    save_histogram(counts, output_dir / "blocked_events_per_crossing_histogram.png")
    save_bar(top_crossings, "Top blocked crossings", "Crossing ID", "Blocked event count", output_dir / "top_blocked_crossings.png")
    save_bar(
        state_counts.head(15),
        "Blocked events by state",
        "State",
        "Blocked event count",
        output_dir / "blocked_events_by_state.png",
        rotation=0,
    )
    save_bar(
        reason_counts.head(10),
        "Blocked event reasons",
        "Reason",
        "Count",
        output_dir / "blocked_event_reasons.png",
        rotation=30,
    )
    save_group_boxplot(
        to_numeric(inventory_with_counts.loc[inventory_with_counts["is_blocked"], "Annual Average Daily Traffic Count"]),
        to_numeric(inventory_with_counts.loc[~inventory_with_counts["is_blocked"], "Annual Average Daily Traffic Count"]),
        "AADT by crossing status",
        "Annual Average Daily Traffic Count",
        output_dir / "aadt_blocked_vs_unblocked.png",
        log_scale=True,
    )
    save_group_boxplot(
        to_numeric(inventory_with_counts.loc[inventory_with_counts["is_blocked"], "Total Daylight Thru Trains"]),
        to_numeric(inventory_with_counts.loc[~inventory_with_counts["is_blocked"], "Total Daylight Thru Trains"]),
        "Daylight train volume by crossing status",
        "Total Daylight Thru Trains",
        output_dir / "daylight_trains_blocked_vs_unblocked.png",
        log_scale=True,
    )
    save_bar(
        state_density.head(15).set_index("State Name")["blocked_crossing_share_pct"],
        "Blocked crossing share by state",
        "State",
        "Blocked crossings / total crossings (%)",
        output_dir / "blocked_crossing_share_by_state.png",
        rotation=0,
    )

    print("Join summary")
    print(pd.DataFrame(summary_rows, columns=["metric", "value"]).to_string(index=False))
    print()
    print("Blocked event count distribution")
    print(summarize_counts(counts).to_string(index=False))
    print()
    print("Top 10 crossings by blocked events")
    print(top_crossings.head(10).to_string())
    print()
    print("Top 10 states by blocked events")
    print(state_counts.head(10).to_string())
    print()
    print("Top 10 states by blocked crossing share")
    print(state_density.head(10).to_string(index=False))
    print()
    print("Blocked vs unblocked numeric comparison")
    print(comparison_table.head(20).to_string(index=False))
    print()
    print(f"Outputs written to: {output_dir}")


if __name__ == "__main__":
    main()

'''
# ==========================================
# VISUALIZATIONS
# ==========================================
print("\nGenerating plots...")

# 1. Plot the Distribution (Log Scale for Sparseness vs. Density)
plt.figure(figsize=(10, 5))
# Replace 'Crossing ID' with the exact column name you used for your join key
block_counts = merged_df['Crossing ID'].value_counts() 

sns.histplot(block_counts, bins=50, color='darkblue', log_scale=(False, True))
plt.title('Distribution of Blocked Events per Crossing (Log Scale)')
plt.xlabel('Number of Blocked Events at a Single Crossing')
plt.ylabel('Number of Crossings (Log Scale)')
plt.grid(True, which="both", ls="--", alpha=0.5)
plt.tight_layout()
plt.savefig('blocked_events_distribution.png')
print("Saved: 'blocked_events_distribution.png'")

# 2. Plot Top 10 States
plt.figure(figsize=(10, 6))
# Replace 'State' with the exact state column name if it's different
state_counts = merged_df['State'].value_counts().head(10) 

sns.barplot(x=state_counts.values, y=state_counts.index, palette='viridis')
plt.title('Top 10 States by Blocked Event Counts')
plt.xlabel('Total Blocked Events Reported')
plt.ylabel('State')
plt.tight_layout()
plt.savefig('top_states_blocked.png')
print("Saved: 'top_states_blocked.png'")

print("\nAll tasks completed successfully!")
'''
