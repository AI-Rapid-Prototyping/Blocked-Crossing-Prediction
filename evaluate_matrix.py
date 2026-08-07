"""
FILE: evaluate_matrix.py
PURPOSE: Tests how well the models predict severe delays (31+ mins vs under 31 mins). 
         creates confusion matrices, and prints the top features driving long blockages.
"""
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import warnings
warnings.filterwarnings('ignore')

from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier
from sklearn.neural_network import MLPClassifier
from sklearn.metrics import (
    confusion_matrix, ConfusionMatrixDisplay, 
    precision_score, recall_score, f1_score, accuracy_score,
    precision_recall_curve
)

# 1. Load Data & Prepare Binary Target ("Is Severe Block >= 31 mins?")
df = pd.read_csv("cleaned_houston_data.csv")

feature_cols_num = ['hour', 'day_of_week', 'month', 'is_weekend', 'crossing_frequency', 'railroad_frequency']
feature_cols_cat = ['Reason']

X = df[feature_cols_num + feature_cols_cat].copy()
X['Reason'] = X['Reason'].fillna('Unknown')

# Create Binary Target: 1 = Severe Block (>= 31 mins), 0 = Short Block (< 31 mins)
severe_classes = ['31-60 minutes', '1-2 hours', 'Over 2 hours']
y = df['target_class'].isin(severe_classes).astype(int)

# Split
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.20, random_state=42, stratify=y)

preprocessor = ColumnTransformer(
    transformers=[
        ('num', StandardScaler(), feature_cols_num),
        ('cat', OneHotEncoder(handle_unknown='ignore'), feature_cols_cat)
    ]
)

# 2. Models
models = {
    "Logistic Regression": Pipeline([('prep', preprocessor), ('clf', LogisticRegression(max_iter=1000))]),
    "Random Forest": Pipeline([('prep', preprocessor), ('clf', RandomForestClassifier(n_estimators=100, max_depth=10, random_state=42))]),
    "Neural Network": Pipeline([('prep', preprocessor), ('clf', MLPClassifier(hidden_layer_sizes=(64, 32), max_iter=500, random_state=42))])
}

# 3. Compute Metrics & Plot Confusion Matrices
fig, axes = plt.subplots(1, 3, figsize=(18, 5))
summary_metrics = []

for i, (name, model) in enumerate(models.items()):
    model.fit(X_train, y_train)
    preds = model.predict(X_test)
    
    acc = accuracy_score(y_test, preds)
    prec = precision_score(y_test, preds)
    rec = recall_score(y_test, preds) # Sensitivity
    f1 = f1_score(y_test, preds)
    
    summary_metrics.append({
        'Model': name,
        'Accuracy': f"{acc:.4f}",
        'Precision': f"{prec:.4f}",
        'Recall (Sensitivity)': f"{rec:.4f}",
        'F1-Score': f"{f1:.4f}"
    })
    
    # Plot Confusion Matrix
    cm = confusion_matrix(y_test, preds)
    disp = ConfusionMatrixDisplay(confusion_matrix=cm, display_labels=['Short Block', 'Severe Block'])
    disp.plot(ax=axes[i], cmap='Blues', colorbar=False)
    axes[i].set_title(f"{name}\nAcc: {acc:.2f} | Prec: {prec:.2f} | Rec: {rec:.2f}")

plt.tight_layout()
plt.savefig("confusion_matrices_comparison.png")
print("Saved plot: confusion_matrices_comparison.png")

# Print Metrics Summary
metrics_df = pd.DataFrame(summary_metrics)
print("\n=== MODEL PERFORMANCE METRICS COMPARISON ===")
print(metrics_df.to_string(index=False))

# 4. Threshold Tuning for Random Forest (Probability Analysis)
rf_model = models["Random Forest"]
y_probs = rf_model.predict_proba(X_test)[:, 1] # Probability of severe block

precisions, recalls, thresholds = precision_recall_curve(y_test, y_probs)

plt.figure(figsize=(8, 5))
plt.plot(thresholds, precisions[:-1], label='Precision', color='blue')
plt.plot(thresholds, recalls[:-1], label='Recall (Sensitivity)', color='green')
plt.xlabel('Probability Threshold (Decision Cutoff)')
plt.ylabel('Score')
plt.title('Random Forest: Precision vs. Recall Across Different Thresholds')
plt.legend()
plt.grid(True)
plt.savefig("threshold_tradeoff.png")
print("Saved plot: threshold_tradeoff.png")

# 5. Extract Variable / Feature Importances
ohe_cols = rf_model.named_steps['prep'].named_transformers_['cat'].get_feature_names_out(feature_cols_cat)
all_features = feature_cols_num + list(ohe_cols)
importances = rf_model.named_steps['clf'].feature_importances_

feat_df = pd.DataFrame({'Variable': all_features, 'Importance': importances}).sort_values(by='Importance', ascending=False)
print("\n=== RANDOM FOREST VARIABLE IMPORTANCE ===")
print(feat_df.head(10).to_string(index=False))