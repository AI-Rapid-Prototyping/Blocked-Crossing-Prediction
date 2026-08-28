import pandas as pd
import numpy as np

print("Reading raw reports.xlsx...")
df = pd.read_excel("reports.xlsx")

# 1. Parse Timestamps
df['Date/Time'] = pd.to_datetime(df['Date/Time'])
df['hour'] = df['Date/Time'].dt.hour
df['day_of_week'] = df['Date/Time'].dt.dayofweek # 0 = Monday, 6 = Sunday
df['month'] = df['Date/Time'].dt.month
df['is_weekend'] = df['day_of_week'].apply(lambda x: 1 if x >= 5 else 0)

# 2. Process Duration Target
# Define canonical buckets for Classification
duration_categories = [
    'Under 15 minutes', 
    '16-30 minutes', 
    '31-60 minutes', 
    '1-2 hours', 
    'Over 2 hours'
]

# Midpoint dictionary for Regression
duration_midpoint_map = {
    'Under 15 minutes': 7.5,
    '16-30 minutes': 23.0,
    '31-60 minutes': 45.5,
    '1-2 hours': 90.0,
    'Over 2 hours': 150.0 # Standard proxy for long delays
}

# Clean string formatting in Duration column
df['Duration_Clean'] = df['Duration'].astype(str).str.strip()

# Target 1: Classification Label
df['target_class'] = df['Duration_Clean']

# Target 2: Regression Continuous Variable (Minutes)
df['target_minutes'] = df['Duration_Clean'].map(duration_midpoint_map)

# Drop rows where duration was unrecognized or missing
df = df.dropna(subset=['target_class', 'target_minutes']).copy()

# 3. Frequency Encoding for High-Cardinality Categoricals
# Crossing ID and Street have many distinct values; frequency encoding represents popularity
crossing_counts = df['Crossing ID'].value_counts()
df['crossing_frequency'] = df['Crossing ID'].map(crossing_counts)

railroad_counts = df['Railroad'].value_counts()
df['railroad_frequency'] = df['Railroad'].map(railroad_counts)

# Save processed dataset
df.to_csv("cleaned_houston_data.csv", index=False)
print(f"Dataset successfully cleaned and saved! Total records: {len(df)}")