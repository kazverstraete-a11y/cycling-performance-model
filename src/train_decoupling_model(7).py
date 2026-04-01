import pandas as pd
import scipy.stats as stats
import matplotlib.pyplot as plt
import seaborn as sns
import shap

from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, r2_score
from sklearn.model_selection import LeaveOneOut, cross_val_predict
from sklearn.linear_model import LinearRegression

# ------------------------------------------------------------------------------------------
# --------------------------------- INITIATE & CLEANING -----------------------------------
# ------------------------------------------------------------------------------------------

ml_df = pd.read_parquet("dataset_final.parquet")

TARGET = "target_decoupling"

train_df = ml_df.dropna(subset=[TARGET]).copy()

other_features = ["CTL", "ATL", "avg_temperature", "start_uur"]
X_cols = [col for col in train_df.columns if "_ewma" in col or col in other_features]

X = train_df[X_cols].reset_index(drop=True)
y = train_df[TARGET].reset_index(drop=True)

print(f"\nTraining decoupling model on {len(train_df)} observations...")

# ------------------------------------------------------------------------------------------
# ------------------------------ VALIDATION: LOOCV ----------------------------------------
# ------------------------------------------------------------------------------------------

model = RandomForestRegressor(n_estimators=500, random_state=42, n_jobs=-1)
loo = LeaveOneOut()

y_pred = cross_val_predict(model, X, y, cv=loo)

mae = mean_absolute_error(y, y_pred)
r2 = r2_score(y, y_pred)

print("\n--- VALIDATION RESULTS (Decoupling) ---")
print(f"MAE: {mae:.4f}")
print(f"R² Score: {r2:.4f}")

results = pd.DataFrame({
    "True": y,
    "Predicted": y_pred
})
print(results.head(20))

# ------------------------------------------------------------------------------------------
# ---------------------------- SCATTER: TRUE VS PREDICTED ---------------------------------
# ------------------------------------------------------------------------------------------

plt.figure(figsize=(10, 8))
plt.scatter(y, y_pred, alpha=0.8, color="blue", label="Observations")

min_val = min(y.min(), y_pred.min())
max_val = max(y.max(), y_pred.max())
plt.plot([min_val, max_val], [min_val, max_val], "r--", label="Perfect Fit")

plt.xlabel("True Decoupling")
plt.ylabel("Predicted Decoupling")
plt.title("LOOCV Alignment: True vs Predicted target_decoupling")
plt.legend()
plt.tight_layout()
plt.show()

# ------------------------------------------------------------------------------------------
# --------------------------- TRAIN FINAL MODEL ON ALL DATA --------------------------------
# ------------------------------------------------------------------------------------------

final_model = RandomForestRegressor(n_estimators=500, random_state=42, n_jobs=-1)
final_model.fit(X, y)

importances = final_model.feature_importances_
importance_df = pd.DataFrame({
    "Feature": X.columns,
    "Importance": importances
}).sort_values("Importance", ascending=False)

top_15 = importance_df.head(15)

plt.figure(figsize=(10, 8))
sns.barplot(x="Importance", y="Feature", data=top_15)
plt.title(f"Top 15 Most Important Features for {TARGET}")
plt.tight_layout()
plt.show()

# ------------------------------------------------------------------------------------------
# --------------------------- STATISTICS: r & p on top features ----------------------------
# ------------------------------------------------------------------------------------------

top_15_features = top_15["Feature"].tolist()

print(f"\n--- Model R²-score: {r2:.4f} ---\n")
print("--- Statistics: Spearman r & p-values on top 15 features ---")
print(f"Target variable: {TARGET} (lower decoupling = better)\n")
print(f"{'Feature':<35} | {'Spearman r':<10} | {'p-value':<10}")
print("-" * 65)

for feature in top_15_features:
    r, p = stats.spearmanr(X[feature], y)
    sig = "***" if p < 0.001 else "**" if p < 0.01 else "*" if p < 0.05 else "n.s."
    print(f"{feature:<35} | {r:>10.3f} | {p:>8.4f} {sig}")

# ------------------------------------------------------------------------------------------
# ------------------------------------ SHAP ANALYSIS --------------------------------------
# ------------------------------------------------------------------------------------------

print("\nCalculating SHAP values...")
explainer = shap.TreeExplainer(final_model)
shap_values = explainer.shap_values(X)

df_shap = pd.DataFrame(shap_values, columns=X.columns)

df_shap_long = df_shap.melt(var_name="Feature", value_name="Impact")
df_X_long = X.melt(var_name="Feature", value_name="Feature_Value")

df_tableau = pd.DataFrame({
    "Feature": df_shap_long["Feature"],
    "Impact": df_shap_long["Impact"],
    "Feature_Value": df_X_long["Feature_Value"]
})

df_tableau.to_csv("shap_values_decoupling.csv", index=False, sep=';')

plt.figure(figsize=(12, 8))
plt.title(f"SHAP Summary: Impact on {TARGET}", fontsize=14, pad=20)
shap.summary_plot(shap_values, X, max_display=15, show=False)
plt.tight_layout()
plt.show()
