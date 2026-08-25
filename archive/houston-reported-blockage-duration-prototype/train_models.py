import pandas as pd
import numpy as np
import warnings
warnings.filterwarnings('ignore')

from sklearn.model_selection import train_test_split, StratifiedKFold, cross_val_score
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier
from sklearn.neural_network import MLPClassifier
from sklearn.metrics import classification_report, accuracy_score

# 1. Load Cleaned Houston Data
print("Loading cleaned_houston_data.csv...")
df = pd.read_csv("cleaned_houston_data.csv")

# Select Numeric & Categorical Features
feature_cols_num = ['hour', 'day_of_week', 'month', 'is_weekend', 'crossing_frequency', 'railroad_frequency']
feature_cols_cat = ['Reason']

X = df[feature_cols_num + feature_cols_cat].copy()
y = df['target_class']

# Clean up missing categorical entries
X['Reason'] = X['Reason'].fillna('Unknown')

# 2. Train / Test Split (Hold out 20% for final evaluation)
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.20, random_state=42, stratify=y
)

print(f"\nTraining set size: {len(X_train)} samples")
print(f"Testing set size: {len(X_test)} samples\n")

# Preprocessing Pipeline: Scale numbers, One-Hot Encode text categories
preprocessor = ColumnTransformer(
    transformers=[
        ('num', StandardScaler(), feature_cols_num),
        ('cat', OneHotEncoder(handle_unknown='ignore'), feature_cols_cat)
    ]
)

# 3. Define Candidate Models
models = {
    "Logistic Regression": Pipeline([
        ('prep', preprocessor),
        ('clf', LogisticRegression(max_iter=1000, random_state=42))
    ]),
    "Random Forest": Pipeline([
        ('prep', preprocessor),
        ('clf', RandomForestClassifier(n_estimators=100, max_depth=10, random_state=42))
    ]),
    "Neural Network (MLP)": Pipeline([
        ('prep', preprocessor),
        ('clf', MLPClassifier(hidden_layer_sizes=(64, 32), max_iter=500, random_state=42))
    ])
}

# 4. Perform 5-Fold Cross-Validation on Training Data
cv = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)

print("--- 5-FOLD CROSS-VALIDATION RESULTS (Train Set) ---")
for name, model_pipeline in models.items():
    scores = cross_val_score(model_pipeline, X_train, y_train, cv=cv, scoring='accuracy')
    print(f"{name:20s} | Mean Accuracy: {scores.mean():.4f} (+/- {scores.std():.4f})")

# 5. Evaluate Final Holdout Performance on Test Set
print("\n--- HOLDOUT TEST SET PERFORMANCE ---")
for name, model_pipeline in models.items():
    # Fit model on entire training set
    model_pipeline.fit(X_train, y_train)
    
    # Predict on unseen test set
    preds = model_pipeline.predict(X_test)
    acc = accuracy_score(y_test, preds)
    
    print(f"\n================ {name} ================")
    print(f"Overall Test Accuracy: {acc:.4f}")
    print(classification_report(y_test, preds, zero_division=0))