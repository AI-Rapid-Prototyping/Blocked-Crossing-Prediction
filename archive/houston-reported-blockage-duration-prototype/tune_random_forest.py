import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import warnings
warnings.filterwarnings('ignore')

from sklearn.model_selection import train_test_split, GridSearchCV, StratifiedKFold
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report, accuracy_score, confusion_matrix, ConfusionMatrixDisplay

# 1. Load Cleaned Houston Data
print("Loading cleaned_houston_data.csv...")
df = pd.read_csv("cleaned_houston_data.csv")

# Features & Target
feature_cols_num = ['hour', 'day_of_week', 'month', 'is_weekend', 'crossing_frequency', 'railroad_frequency']
feature_cols_cat = ['Reason']

X = df[feature_cols_num + feature_cols_cat].copy()
y = df['target_class']

# Fill missing categorical values
X['Reason'] = X['Reason'].fillna('Unknown')

# 2. Train / Test Split (80% Train, 20% Holdout Test)
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.20, random_state=42, stratify=y
)

# 3. Preprocessing Pipeline
preprocessor = ColumnTransformer(
    transformers=[
        ('num', StandardScaler(), feature_cols_num),
        ('cat', OneHotEncoder(handle_unknown='ignore'), feature_cols_cat)
    ]
)

# 4. Create Pipeline with Random Forest
rf_pipeline = Pipeline([
    ('prep', preprocessor),
    ('clf', RandomForestClassifier(random_state=42))
])

# 5. Define Hyperparameter Grid for Tuning
param_grid = {
    'clf__n_estimators': [100, 200, 300],
    'clf__max_depth': [8, 12, 16, None],
    'clf__min_samples_split': [2, 5, 10],
    'clf__criterion': ['gini', 'entropy']
}

# 6. Grid Search with 5-Fold Cross-Validation
print("\nStarting GridSearchCV to find optimal Random Forest parameters...")
cv = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)

grid_search = GridSearchCV(
    estimator=rf_pipeline,
    param_grid=param_grid,
    cv=cv,
    scoring='accuracy',
    n_jobs=-1, # Use all available CPU cores
    verbose=1
)

grid_search.fit(X_train, y_train)

print(f"\nBest Cross-Validation Accuracy: {grid_search.best_score_:.4f}")
print("Best Parameters Found:")
for param, val in grid_search.best_params_.items():
    print(f"  - {param.replace('clf__', '')}: {val}")

# 7. Evaluate Best Model on Unseen Test Set
best_model = grid_search.best_estimator_
y_pred = best_model.predict(X_test)
test_acc = accuracy_score(y_test, y_pred)

print("\n================ FINAL TUNED RANDOM FOREST TEST RESULTS ================")
print(f"Test Set Accuracy: {test_acc:.4f}\n")
print(classification_report(y_test, y_pred, zero_division=0))

# 8. Feature Importance Extraction & Plotting
ohe_cols = best_model.named_steps['prep'].named_transformers_['cat'].get_feature_names_out(feature_cols_cat)
all_features = feature_cols_num + list(ohe_cols)
importances = best_model.named_steps['clf'].feature_importances_

feature_df = pd.DataFrame({'Feature': all_features, 'Importance': importances})
feature_df = feature_df.sort_values(by='Importance', ascending=False)

print("\n--- TOP FEATURE IMPORTANCES ---")
print(feature_df.head(10).to_string(index=False))

# Save Feature Importance Plot
plt.figure(figsize=(10, 6))
sns.barplot(data=feature_df.head(10), x='Importance', y='Feature', palette='Blues_r')
plt.title('Top 10 Drivers of Houston Train Blockage Duration (Random Forest)')
plt.xlabel('Relative Feature Importance')
plt.tight_layout()
plt.savefig('feature_importance.png')
print("\nSaved plot: feature_importance.png")

# Save Confusion Matrix Plot
plt.figure(figsize=(8, 6))
cm = confusion_matrix(y_test, y_pred, labels=best_model.classes_)
disp = ConfusionMatrixDisplay(confusion_matrix=cm, display_labels=best_model.classes_)
disp.plot(cmap='Blues', values_format='d')
plt.title('Random Forest Confusion Matrix')
plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig('confusion_matrix.png')
print("Saved plot: confusion_matrix.png")