from pathlib import Path
import geopandas as gpd
import pandas as pd

# Setup repository paths
repo_root = Path(r"C:/Projects/Blocked-Crossing-Prediction") 

# Paths relative to project root
reports_path = repo_root / "data" / "reports.xlsx"
yards_path = repo_root / "data" / "rail_yards.geojson"
network_path = repo_root / "data" / "rail_network_lines.geojson"
inventory_path = repo_root / "data" / "Crossing_Inventory_Data_(Form_71)_-_Current_20260707.csv"
waze_path = repo_root / "data" / "251013_waze_harris_county.csv"
output_path = repo_root / "analysis_outputs" / "master_model_input.csv"

def resolve_column(df: pd.DataFrame, candidates: list[str]) -> str:
    lookup = {col.lower(): col for col in df.columns}
    for candidate in candidates:
        if candidate.lower() in lookup:
            return lookup[candidate.lower()]
    raise KeyError(f"None of these columns were found: {candidates}")

# ==============================================================================
# 1. Load Event Reports & Parse Timestamps / Targets
# ==============================================================================
print("1/5 Loading and parsing event reports...")
df = pd.read_excel(reports_path)

crossing_id_col = resolve_column(df, ["Crossing ID", "crossing_id"])
df[crossing_id_col] = df[crossing_id_col].astype(str).str.strip()

# Timestamps & Temporal Features
df['Date/Time'] = pd.to_datetime(df['Date/Time'])
df['hour'] = df['Date/Time'].dt.hour
df['day_of_week'] = df['Date/Time'].dt.dayofweek
df['month'] = df['Date/Time'].dt.month
df['is_weekend'] = (df['day_of_week'] >= 5).astype(int)

# Target Setup (Binary & Midpoints)
duration_midpoint_map = {
    'Under 15 minutes': 7.5,
    '16-30 minutes': 23.0,
    '31-60 minutes': 45.5,
    '1-2 hours': 90.0,
    'Over 2 hours': 150.0
}
df['Duration_Clean'] = df['Duration'].astype(str).str.strip()
df['target_class'] = df['Duration_Clean']
df['target_minutes'] = df['Duration_Clean'].map(duration_midpoint_map)

# Simple Event Frequencies
df['crossing_frequency'] = df[crossing_id_col].map(df[crossing_id_col].value_counts())
df['railroad_frequency'] = df['Railroad'].map(df['Railroad'].value_counts())

# ==============================================================================
# 2. Extract Static GIS Features (Rail Yards & Lines)
# ==============================================================================
print("2/5 Computing spatial proximity to yards and rail networks...")

# Load Form 71 inventory with encoding fix for hidden byte markers
inventory = pd.read_csv(inventory_path, encoding="utf-8-sig", low_memory=False)
inventory.columns = inventory.columns.str.strip()

# Resolve columns on INVENTORY (where spatial coordinates live)
inv_id_col = resolve_column(inventory, ["Crossing ID", "crossing_id"])
lat_col = resolve_column(inventory, ["Latitude", "LATITUDE", "latitude"])
lon_col = resolve_column(inventory, ["Longitude", "LONGITUDE", "longitude"])
revision_col = resolve_column(inventory, ["Revision Date", "revision date"])

# Clean IDs and keep latest revision
inventory[inv_id_col] = inventory[inv_id_col].astype(str).str.strip()
inventory[revision_col] = pd.to_datetime(inventory[revision_col], errors="coerce")
inventory_latest = inventory.sort_values([inv_id_col, revision_col]).drop_duplicates(subset=[inv_id_col], keep="last")

# Extract numeric coordinates and drop missing rows
crossings_coords = inventory_latest[[inv_id_col, lat_col, lon_col]].copy()
crossings_coords[lat_col] = pd.to_numeric(crossings_coords[lat_col], errors="coerce")
crossings_coords[lon_col] = pd.to_numeric(crossings_coords[lon_col], errors="coerce")
crossings_coords = crossings_coords.dropna(subset=[lat_col, lon_col])

# Build GeoDataFrame
crossings_gdf = gpd.GeoDataFrame(
    crossings_coords,
    geometry=gpd.points_from_xy(crossings_coords[lon_col], crossings_coords[lat_col]),
    crs="EPSG:4326"
).to_crs("EPSG:2163")

crossings_gdf = crossings_gdf.rename(columns={inv_id_col: crossing_id_col})

# A. Yards Proximity
yards = gpd.read_file(yards_path).to_crs("EPSG:2163")
yard_nearest = gpd.sjoin_nearest(crossings_gdf, yards, how="left", distance_col="dist_m")
yard_nearest = yard_nearest.sort_values([crossing_id_col, "dist_m"]).drop_duplicates(subset=[crossing_id_col])

crossings_gdf["yard_buffer"] = crossings_gdf.geometry.buffer(8046.72) # 5 miles
yard_counts = gpd.sjoin(crossings_gdf.set_geometry("yard_buffer"), yards, how="left", predicate="intersects")
yard_counts_df = yard_counts.groupby(crossing_id_col).size().rename("yards_within_5_miles").reset_index()

yard_features = yard_nearest[[crossing_id_col, "dist_m"]].merge(yard_counts_df, on=crossing_id_col, how="left")
yard_features["distance_to_nearest_yard_miles"] = yard_features["dist_m"] / 1609.344
yard_features = yard_features.drop(columns=["dist_m"])

# B. Rail Network Lines Proximity
network = gpd.read_file(network_path).to_crs("EPSG:2163")
network["PASSNGR"] = network["PASSNGR"].astype("string").str.strip().str.upper()
network["is_passenger_line"] = network["PASSNGR"].isin(["A", "B", "C", "E", "I", "R", "T"])

net_nearest = gpd.sjoin_nearest(crossings_gdf.set_geometry("geometry"), network[["geometry", "NET", "TRACKS"]], how="left", distance_col="dist_net_m")
net_nearest = net_nearest.sort_values([crossing_id_col, "dist_net_m"]).drop_duplicates(subset=[crossing_id_col])

pass_nearest = gpd.sjoin_nearest(crossings_gdf.set_geometry("geometry"), network.loc[network["is_passenger_line"], ["geometry"]], how="left", distance_col="dist_pass_m")
pass_nearest = pass_nearest.sort_values([crossing_id_col, "dist_pass_m"]).drop_duplicates(subset=[crossing_id_col])

gis_summary = yard_features.merge(
    net_nearest[[crossing_id_col, "NET", "TRACKS", "dist_net_m"]], on=crossing_id_col, how="left"
).merge(
    pass_nearest[[crossing_id_col, "dist_pass_m"]], on=crossing_id_col, how="left"
)

gis_summary["dist_to_nearest_network_line_miles"] = gis_summary["dist_net_m"] / 1609.344
gis_summary["dist_to_nearest_passenger_line_miles"] = gis_summary["dist_pass_m"] / 1609.344
gis_summary = gis_summary.rename(columns={"NET": "nearest_network_code", "TRACKS": "nearest_track_count"})
gis_summary = gis_summary.drop(columns=["dist_net_m", "dist_pass_m"])

# ==============================================================================
# 3. Pull Physical Form 71 Inventory Attributes
# ==============================================================================
print("3/5 Attaching Form 71 crossing physical inventory attributes...")
inventory = pd.read_csv(inventory_path, low_memory=False)
inv_id_col = resolve_column(inventory, ["Crossing ID", "crossing_id"])
inventory[inv_id_col] = inventory[inv_id_col].astype(str).str.strip()

inv_cols = {
    inv_id_col: crossing_id_col,
    "Smallest Crossing Angle": "inv_smallest_crossing_angle",
    "Gate Configuration": "inv_gate_configuration",
    "Track Signaled": "inv_track_signaled",
    "Reporting Railroad Class": "inv_reporting_railroad_class"
}
existing_inv_cols = [c for c in inv_cols.keys() if c in inventory.columns]
inventory_clean = inventory[existing_inv_cols].rename(columns=inv_cols).drop_duplicates(subset=[crossing_id_col])

# ==============================================================================
# 4. Spatiotemporal Join with Waze Incidents & Duration Extraction
# ==============================================================================
print("4/5 Performing 300m spatiotemporal join with Waze traffic data...")
if waze_path.exists():
    waze_df = pd.read_csv(waze_path)
    
    # 1. Parse timestamps cleanly using format='mixed' to avoid UserWarnings
    waze_df['waze_start'] = pd.to_datetime(waze_df['Start time'], format='mixed', errors='coerce')
    if waze_df['waze_start'].dt.tz is not None:
        waze_df['waze_start'] = waze_df['waze_start'].dt.tz_localize(None)

    # 2. Extract numeric incident clearance duration in minutes
    duration_col = "Duration (Incident clearance time)"
    if duration_col in waze_df.columns:
        waze_df['waze_duration_mins'] = waze_df[duration_col].astype(str).str.extract(r'(\d+)').astype(float)
    else:
        waze_df['waze_duration_mins'] = np.nan

    # 3. Create spatial GeoDataFrame
    gdf_waze = gpd.GeoDataFrame(
        waze_df,
        geometry=gpd.points_from_xy(waze_df['Longitude'], waze_df['Latitude']),
        crs="EPSG:4326"
    ).to_crs("EPSG:2163")

    # Buffer crossings by 300 meters (~984 feet)
    crossings_300m = crossings_gdf.copy()
    crossings_300m['buffer_300m'] = crossings_300m.geometry.buffer(300)

    # Perform spatial join
    spatial_waze = gpd.sjoin(
        crossings_300m.set_geometry('buffer_300m'),
        gdf_waze[['Event ID', 'Standardized Type', 'waze_start', 'waze_duration_mins', 'Reliability Score', 'geometry']],
        how='left',
        predicate='contains'
    )

    # 4. Calculate temporal window matches (-15m to +5m)
    waze_features_list = []
    
    for idx, row in df.iterrows():
        c_id = row[crossing_id_col]
        e_time = row['Date/Time']
        
        matches = spatial_waze[spatial_waze[crossing_id_col] == c_id]
        
        if not matches.empty and matches['waze_start'].notna().any():
            matches = matches.copy()
            matches['time_diff_mins'] = (matches['waze_start'] - e_time).dt.total_seconds() / 60.0
            
            valid = matches[(matches['time_diff_mins'] >= -15.0) & (matches['time_diff_mins'] <= 5.0)]
            
            if not valid.empty:
                waze_features_list.append({
                    crossing_id_col: c_id,
                    'Date/Time': e_time,
                    'has_waze_alert': 1,
                    'waze_alert_count': len(valid),
                    'waze_avg_duration_mins': valid['waze_duration_mins'].mean(),
                    'waze_max_reliability': valid['Reliability Score'].max()
                })
            else:
                waze_features_list.append({crossing_id_col: c_id, 'Date/Time': e_time, 'has_waze_alert': 0, 'waze_alert_count': 0, 'waze_avg_duration_mins': 0.0, 'waze_max_reliability': 0})
        else:
            waze_features_list.append({crossing_id_col: c_id, 'Date/Time': e_time, 'has_waze_alert': 0, 'waze_alert_count': 0, 'waze_avg_duration_mins': 0.0, 'waze_max_reliability': 0})

    waze_summary = pd.DataFrame(waze_features_list)
else:
    print("Notice: Waze dataset file not found. Setting Waze default flags to 0.")
    waze_summary = df[[crossing_id_col, 'Date/Time']].copy()
    waze_summary['has_waze_alert'] = 0
    waze_summary['waze_alert_count'] = 0
    waze_summary['waze_avg_duration_mins'] = 0.0
    waze_summary['waze_max_reliability'] = 0

# ==============================================================================
# 5. Master Merge & Export
# ==============================================================================
print("5/5 Merging into final master dataset...")
master_df = df.merge(gis_summary, on=crossing_id_col, how="left")
master_df = master_df.merge(inventory_clean, on=crossing_id_col, how="left")
master_df = master_df.merge(waze_summary, on=[crossing_id_col, 'Date/Time'], how="left")

# Save output
output_path.parent.mkdir(parents=True, exist_ok=True)
master_df.to_csv(output_path, index=False)

print(f"Success! Master dataset exported to: {output_path}")
print(f"Total Rows: {len(master_df):,} | Total Features: {len(master_df.columns)}")