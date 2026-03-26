import pandas as pd
import scipy.stats as stats
import matplotlib.pyplot as plt
import seaborn as sns
import shap

from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, r2_score
from sklearn.model_selection import KFold, cross_val_predict
from sklearn.linear_model import LinearRegression


# ------------------------------------------------------------------------------------------
# --------------------------------- INITIATE & CLEANING -----------------------------------
# ------------------------------------------------------------------------------------------

ml_df = pd.read_parquet("alex_ml_dataset_final.parquet")

TARGET = "target_race_EF"

train_df = ml_df.dropna(subset=[TARGET]).copy()

other_features = ["strenght_yesterday", "CTL", "ATL", "avg_temperature", "start_uur"]
X_cols = [col for col in train_df.columns if "_ewma" in col or col in other_features]

X = train_df[X_cols].reset_index(drop=True)
y = train_df[TARGET].reset_index(drop=True)

print(f"\nTraining EF model on {len(train_df)} observations...")


# ------------------------------------------------------------------------------------------
# ------------------------------ VALIDATION: 5-FOLD CV ------------------------------------
# ------------------------------------------------------------------------------------------

kf = KFold(n_splits=5, shuffle=True, random_state=42)
model = RandomForestRegressor(n_estimators=500, random_state=42, n_jobs=-1)

y_pred = cross_val_predict(model, X, y, cv=kf)

mae = mean_absolute_error(y, y_pred)
r2 = r2_score(y, y_pred)

print("\n--- VALIDATION RESULTS (Efficiency Factor) ---")
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

plt.xlabel("True Efficiency Factor")
plt.ylabel("Predicted Efficiency Factor")
plt.title("5-Fold CV Alignment: True vs Predicted target_race_EF")
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
print(f"Target variable: {TARGET}\n")
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

# df_tableau.to_csv("shap_values_ef.csv", index=False, sep=';')

plt.figure(figsize=(12, 8))
plt.title(f"SHAP Summary: Impact on {TARGET}", fontsize=14, pad=20)
shap.summary_plot(shap_values, X, max_display=15, show=False)
plt.tight_layout()
plt.show()


# ------------------------------------------------------------------------------------------
# ------------------------- LINEAR REGRESSION FOR TABLEAU SLIDERS --------------------------
# ------------------------------------------------------------------------------------------

top_5_features = importance_df.head(5)["Feature"].tolist()

X_sim = train_df[top_5_features].dropna()
y_sim = train_df.loc[X_sim.index, TARGET]

lr = LinearRegression()
lr.fit(X_sim, y_sim)

print("\n--- TABLEAU WHAT-IF SLIDER VALUES ---")
print(f"Intercept: {lr.intercept_:.4f}")

for feature, coef in zip(top_5_features, lr.coef_):
    print(f"{feature}: {coef:.6f}")
