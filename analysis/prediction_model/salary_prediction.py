import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import streamlit as st
from scipy.stats import skew
from sklearn.linear_model import LinearRegression
from .machine_learning_model import MLModelHandler
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
from sklearn.svm import SVR
from sklearn.metrics import r2_score
from pathlib import Path

try:
    from xgboost import XGBRegressor
    HAS_XGB = True
except Exception:
    HAS_XGB = False

class SalaryPredictionPipeline:
    """
    Salary Prediction Pipeline
    Step-based implementation for Streamlit ML page
    """
    def __init__(self):
        self.model_handler = MLModelHandler() # Khởi tạo handler mới

    # --- STEP 1: DIAGNOSTIC REPORT ---
    def step_1_data_diagnostic(
        self,
        df: pd.DataFrame,
        min_salary_col="min_salary",
        max_salary_col="max_salary",
        date_col="posted_date",
    ):
        import numpy as np
        import matplotlib.pyplot as plt

        df_run = df.copy()

        # ------------------------------------------------------
        # TARGET DEFINITION
        # ------------------------------------------------------

        required_cols = {min_salary_col, max_salary_col}
        missing = required_cols - set(df_run.columns)

        if missing:
            raise RuntimeError(f"Missing required salary columns: {missing}")

        df_run["avg_salary"] = df_run[[min_salary_col, max_salary_col]].mean(axis=1)

        y = df_run["avg_salary"]
        y_clean = y.dropna()

        total_rows = len(df_run)
        non_null_rows = len(y_clean)
        null_rows = y.isna().sum()
        variance = float(y_clean.var()) if non_null_rows > 0 else 0.0

        # ------------------------------------------------------
        # FEASIBILITY CHECK
        # ------------------------------------------------------

        MIN_REQUIRED_SAMPLES = 24

        if non_null_rows < MIN_REQUIRED_SAMPLES:
            raise RuntimeError("Not enough samples for supervised learning")

        if variance <= 0:
            raise RuntimeError("Target has zero variance")

        # ------------------------------------------------------
        # MODEL FEASIBILITY ANALYSIS
        # ------------------------------------------------------

        non_null_ratio = non_null_rows / total_rows

        report_lines = []
        report_lines.append("=== STEP 1: TARGET DEFINITION & FEASIBILITY CHECK ===\n")

        report_lines.append("Target definition:")
        report_lines.append(" - avg_salary = (min_salary + max_salary) / 2")
        report_lines.append(f" - Total rows           : {total_rows}")
        report_lines.append(f" - Non-null target rows : {non_null_rows}")
        report_lines.append(f" - Null target rows     : {null_rows}")
        report_lines.append(f" - Target variance      : {round(variance, 2)}\n")

        report_lines.append("Model feasibility analysis:")
        report_lines.append(f" - Non-null target ratio : {non_null_ratio:.4%}")
        report_lines.append(f" - Target variance       : {round(variance, 2)}")

        if non_null_ratio < 0.05:
            report_lines.append(
                "- Extremely low target availability (< 5%) makes "
                "feature-based supervised regression impractical."
            )

        report_lines.append(
            "- The target variable exhibits non-zero variance, "
            "indicating the presence of a learnable signal."
        )

        report_lines.append(
            "- No stable explanatory features have been validated "
            "for direct supervised learning at this stage."
        )

        report_lines.append(
            "- The prediction problem is preliminarily framed as a "
            "TIME-SERIES / TREND-BASED task."
        )

        report_lines.append("\nPreliminary model suggestion:")
        report_lines.append(
            " ✔️️ Trend-oriented time-series models "
            "(e.g., Exponential Smoothing / Holt)."
        )
        report_lines.append(
            " ❌ Feature-based regression models are not suggested "
            "due to insufficient target coverage."
        )

        report_lines.append("\nDecision:")
        report_lines.append(" ✔️️ Proceed to the next step.")

        report = "\n".join(report_lines)

        # ------------------------------------------------------
        # CHART – TARGET DISTRIBUTION (DIAGNOSTIC VIEW)
        # ------------------------------------------------------

        fig, axes = plt.subplots(1, 2, figsize=(12, 4))

        # Histogram (distribution shape)
        axes[0].hist(y_clean, bins=40)
        axes[0].set_title("Average Salary Distribution")
        axes[0].set_xlabel("Salary")
        axes[0].set_ylabel("Frequency")
        axes[0].grid(True)

        # Boxplot (spread & outliers)
        axes[1].boxplot(y_clean, vert=False)
        axes[1].set_title("Average Salary Spread & Outliers")
        axes[1].set_xlabel("Salary")
        axes[1].grid(True)

        plt.tight_layout()


        # ------------------------------------------------------
        # RETURN (STRICT CONTRACT)
        # ------------------------------------------------------

        return df_run, fig, report


    # --- STEP 2: BASELINE REPORT ---
    def step_2_baseline_training(
        self,
        df: pd.DataFrame,
        model_type: str,
        time_granularity: str = "yearly",
    ):

        print("\n=== STEP 2: BASELINE TRAINING & DATA SUITABILITY ===")

        df_run = df.copy()

        # --------------------------------------------------
        # PRE-CHECK
        # --------------------------------------------------
        if "posted_date" not in df_run.columns:
            raise RuntimeError("Missing required column: posted_date")

        if "avg_salary" not in df_run.columns:
            raise RuntimeError("Missing required column: avg_salary")

        # --------------------------------------------------
        # NORMALIZE DATE (YEAR-SAFE)
        # --------------------------------------------------
        s = df_run["posted_date"]
        if pd.api.types.is_numeric_dtype(s):
            df_run["posted_date"] = pd.to_datetime(
                s.astype(str) + "-01-01",
                errors="coerce"
            )
        else:
            df_run["posted_date"] = pd.to_datetime(
                s.astype(str),
                errors="coerce"
            )

        df_run = df_run.dropna(subset=["posted_date", "avg_salary"])
        df_run["year"] = df_run["posted_date"].dt.year

        # --------------------------------------------------
        # AGGREGATE YEARLY
        # --------------------------------------------------
        df_agg = (
            df_run
            .groupby("year")
            .agg(
                avg_salary=("avg_salary", "mean"),
                job_count=("avg_salary", "count")
            )
            .reset_index()
            .sort_values("year")
        )

        df_agg["time_index"] = np.arange(len(df_agg))
        data_out = df_agg.copy()

        # --------------------------------------------------
        # DESCRIPTIVE STATISTICS (FOR SUITABILITY REPORT)
        # --------------------------------------------------
        salary_mean = df_agg["avg_salary"].mean()
        salary_std  = df_agg["avg_salary"].std()
        salary_var  = df_agg["avg_salary"].var()
        cv = salary_std / salary_mean if salary_mean != 0 else 0.0


        year_min = df_agg["year"].min()
        year_max = df_agg["year"].max()
        n_years = df_agg["year"].nunique()

        # --------------------------------------------------
        # BASELINE REGRESSION (TREND PROBE)
        # --------------------------------------------------
        X = df_agg[["time_index"]]
        y = df_agg["avg_salary"]

        model = LinearRegression()
        model.fit(X, y)
        y_pred = model.predict(X)

        r2 = r2_score(y, y_pred)

        residual = y - y_pred
        var_trend = np.var(y_pred)
        var_resid = np.var(residual)

        tnr = var_trend / var_resid if var_resid > 0 else 0.0

        # --------------------------------------------------
        # ENRICHMENT CRITERIA (3 CORE METRICS)
        # --------------------------------------------------
        year_counts = df_run["year"].value_counts(normalize=True)
        dominant_ratio = year_counts.max()

        # ---- DECISION LOGIC ----
        if n_years < 3:
            enrich_status = "❌ FAIL"
            enrich_reason = "Insufficient temporal support (less than 3 years)."

        elif dominant_ratio > 0.8:
            enrich_status = "❌ FAIL"
            enrich_reason = (
                f"Signal is highly concentrated in one year "
                f"(dominant ratio = {dominant_ratio:.2%})."
            )

        elif tnr < 0.3:
            enrich_status = "❌ FAIL"
            enrich_reason = (
                f"No meaningful trend detected (TNR = {tnr:.2f})."
            )

        elif dominant_ratio > 0.6 or tnr < 0.6:
            enrich_status = "⚠️ WEAK"
            enrich_reason = (
                "Temporal signal exists but is weak or imbalanced. "
                "Enrichment may add limited value."
            )

        else:
            enrich_status = "✔️ PASS"
            enrich_reason = (
                "Sufficient temporal support with balanced signal "
                "and detectable trend."
            )
            

        # --------------------------------------------------
        # PRINT SUMMARY (CONSOLE)
        # --------------------------------------------------
        print("Data suitability check:")
        print(f" - Year span          : {year_min} → {year_max}")
        print(f" - Number of years    : {n_years}")
        print(f" - Dominant ratio     : {dominant_ratio:.2%}")
        print(f" - Trend-to-noise (TNR): {tnr:.2f}")
        print(f" - Enrichment status  : {enrich_status}")
        print(f" - Reason             : {enrich_reason}")

        print("Salary distribution statistics:")
        print(f" - Mean salary : {round(salary_mean, 2)}")
        print(f" - Std dev     : {round(salary_std, 2)}")
        print(f" - Variance    : {round(salary_var, 2)}")
        print(f" - CV          : {cv:.2%}")

        # --------------------------------------------------
        # REPORT (FOR UI)
        # --------------------------------------------------
        report = {
            "baseline": f"""
                Internal aggregated data summary:
                - Time granularity : yearly
                - Number of periods: {n_years}
                - Time span        : {year_min} → {year_max}
                - Avg jobs / period: {round(df_agg['job_count'].mean(), 1)}

                Baseline regression (time-only feature):
                - Model used : Linear Regression
                - R²         : {round(r2, 4)}

                Decision:
                ❌ Internal salary data NOT suitable as primary forecasting source.
                """.strip(),

            "suitability": f"""
                Data suitability for temporal enrichment:

                Core criteria:
                1) Temporal support (n_years ≥ 3)
                - n_years = {n_years}

                2) Temporal dominance ratio
                - dominant_ratio = {dominant_ratio:.2%}

                3) Trend-to-noise ratio (TNR)
                - TNR = {tnr:.2f}

                4) Salary distribution summary:
                - Mean salary : {round(salary_mean, 2)}
                - Std dev     : {round(salary_std, 2)}
                - Variance    : {round(salary_var, 2)}
                - CV          : {cv:.2%}

                Enrichment decision:
                {enrich_status}
                Reason:
                - {enrich_reason}
                """.strip()
            }

        report["_meta"] = {
            "enrichment_status": enrich_status,
            "enrichment_reason": enrich_reason,
        }

        # --------------------------------------------------
        # CHART
        # --------------------------------------------------
        fig, ax = plt.subplots(figsize=(10, 4))
        ax.plot(df_agg["year"], y, marker="o", label="Yearly avg salary")
        ax.plot(df_agg["year"], y_pred, linestyle="--", label="Trend probe")
        ax.set_title("Baseline Salary Trend (Yearly)")
        ax.set_xlabel("Year")
        ax.set_ylabel("Average Salary")
        ax.legend()
        ax.grid(True)
        plt.tight_layout()

        return data_out, fig, report


    # --- STEP 3: ENRICHMENT REPORT ---
    def step_3_enrichment(
        self,
        df: pd.DataFrame,
        prev_report: dict | None = None,
    ):

        print("\n=== STEP 3: MONTHLY SALARY ENRICHMENT ===")

        print("""
            Purpose:
            - Convert aggregated yearly salary data into a continuous monthly series.
            - Increase temporal resolution for downstream forecasting models.
            - No forecasting is performed in this step.
            """)
        
        df_run = df.copy()
        enrich_status = None
        enrich_reason = None
    

        if prev_report and isinstance(prev_report, dict) and "_meta" in prev_report:
            enrich_status = prev_report["_meta"].get("enrichment_status")
            enrich_reason = prev_report["_meta"].get("enrichment_reason")


        # --------------------------------------------------
        # PRE-CHECK (PIPELINE-AWARE)
        # --------------------------------------------------
        if "avg_salary" not in df_run.columns:
            raise RuntimeError("Missing required column: avg_salary")

        if "period" in df_run.columns:
            # period is Period[Y]
            df_run["year"] = df_run["period"].astype(str).astype(int)

        elif "year" in df_run.columns:
            df_run["year"] = df_run["year"].astype(int)

        else:
            raise RuntimeError(
                "Missing required time column: expected 'period' or 'year'"
            )

        # --------------------------------------------------
        # AGGREGATE YEARLY (DEFENSIVE)
        # --------------------------------------------------
        df_yearly = (
            df_run
            .groupby("year")
            .agg(yearly_avg_salary=("avg_salary", "mean"))
            .reset_index()
            .sort_values("year")
        )

        # Assign mid-year timestamp
        df_yearly["date"] = pd.to_datetime(
            df_yearly["year"].astype(str) + "-06-01"
        )
        df_yearly = df_yearly.set_index("date")

        # --------------------------------------------------
        # MONTHLY EXPANSION + INTERPOLATION
        # --------------------------------------------------
        monthly_index = pd.date_range(
            start=df_yearly.index.min(),
            end=df_yearly.index.max(),
            freq="MS"
        )

        df_monthly = df_yearly[["yearly_avg_salary"]].reindex(monthly_index)

        df_monthly["monthly_enriched_salary"] = (
            df_monthly["yearly_avg_salary"]
            .interpolate()
        )

        df_monthly = df_monthly.reset_index()
        df_monthly.rename(columns={"index": "date"}, inplace=True)

        df_monthly["year"] = df_monthly["date"].dt.year
        df_monthly["month"] = df_monthly["date"].dt.month

        # --------------------------------------------------
        # LOG SUMMARY
        # --------------------------------------------------
        print("Monthly series created:")
        print(" - Months   :", len(df_monthly))
        print(
            " - Time span:",
            df_monthly["date"].min().date(),
            "→",
            df_monthly["date"].max().date()
        )

        # --------------------------------------------------
        # BUILD DECISION BLOCK BASED ON STEP 2
        # --------------------------------------------------
        if enrich_status == "❌ FAIL":
            decision_block = f"""
                Decision:
                ❌ Monthly salary enrichment is NOT meaningful.

                Reason:
                - {enrich_reason}

                Note:
                - Monthly series was generated successfully.
                - Output is for inspection only and NOT suitable for forecasting.
                """.strip()

        elif enrich_status == "⚠️ WEAK":
            decision_block = f"""
                Decision:
                ⚠️ Monthly salary enrichment is WEAK.

                Reason:
                - {enrich_reason}

                Note:
                - Forecasting may be unstable.
                - Use with caution.
                """.strip()

        elif enrich_status == "✔️ PASS":
            decision_block = """
                Decision:
                ✔️ Monthly salary enrichment successful.
                ✔️ Data is ready for forecasting models.
                """.strip()

        else:
            decision_block = """
                Decision:
                ℹ️ Monthly series generated.
                ℹ️ No prior enrichment assessment available.
                """.strip()

        report = f"""
            === STEP 3: MONTHLY SALARY ENRICHMENT ===

            Purpose:
            - Convert yearly salary data into a continuous monthly series.
            - Increase temporal resolution for time-series modeling.
            - No forecasting is performed in this step.

            Enrichment summary:
            - Source granularity : Yearly
            - Target granularity : Monthly
            - Total months       : {len(df_monthly)}
            - Time span          : {df_monthly['date'].min().date()} → {df_monthly['date'].max().date()}

            Interpretation:
            - Monthly interpolation smooths transitions between yearly points.
            - Temporal structure is preserved without introducing forecast bias.

            {decision_block}
            """.strip()

        # --------------------------------------------------
        # CHART – YEARLY VS MONTHLY
        # --------------------------------------------------
        fig, ax = plt.subplots(figsize=(10, 4))

        ax.plot(
            df_yearly.index,
            df_yearly["yearly_avg_salary"],
            marker="o",
            label="Yearly average salary"
        )

        ax.plot(
            df_monthly["date"],
            df_monthly["monthly_enriched_salary"],
            linestyle="--",
            label="Monthly enriched salary"
        )

        ax.set_title("Salary Trend: Yearly vs Monthly Enriched")
        ax.set_xlabel("Time")
        ax.set_ylabel("Average Salary")
        ax.legend()
        ax.grid(True)
        plt.tight_layout()

        # --------------------------------------------------
        # RETURN (CONTRACT SAFE)
        # --------------------------------------------------
        data_out = df_monthly.copy()

        return data_out, fig, report


    # --- STEP 4: FINAL REPORT ---
    def step_4_forecasting_holt(
        self,
        df: pd.DataFrame,
        prev_report: dict | None = None,
    ):
        print("\n=== STEP 4: FORECASTING MODEL TRAINING (HOLT) ===")

        from statsmodels.tsa.holtwinters import Holt
        from sklearn.metrics import mean_absolute_error, mean_squared_error
        import numpy as np
        import pandas as pd
        import matplotlib.pyplot as plt

        print("""
    Purpose:
    - Learn long-term salary trend from monthly enriched data.
    - Use a time-series model suitable for short-horizon forecasting.
    """)

        df_run = df.copy()

        # --------------------------------------------------
        # READ ENRICHMENT ASSESSMENT FROM STEP 2
        # --------------------------------------------------
        enrich_status = None
        enrich_reason = None

        if prev_report and isinstance(prev_report, dict) and "_meta" in prev_report:
            enrich_status = prev_report["_meta"].get("enrichment_status")
            enrich_reason = prev_report["_meta"].get("enrichment_reason")

        # --------------------------------------------------
        # PREPARE SERIES
        # --------------------------------------------------
        if "date" not in df_run.columns or "monthly_enriched_salary" not in df_run.columns:
            raise RuntimeError(
                "Missing required columns: 'date' and 'monthly_enriched_salary'"
            )

        y_series = (
            df_run
            .set_index("date")["monthly_enriched_salary"]
            .asfreq("MS")
        )

        last_year = y_series.index.year.max()

        y_train = y_series[y_series.index.year < last_year]
        y_test  = y_series[y_series.index.year == last_year]

        print("Training years:", sorted(y_train.index.year.unique()))
        print("Test year     :", last_year)
        print("Train months  :", len(y_train))
        print("Test months   :", len(y_test))

        # --------------------------------------------------
        # FIT HOLT MODEL
        # --------------------------------------------------
        holt = Holt(y_train, damped_trend=True).fit(
            smoothing_level=0.3,
            smoothing_trend=0.1,
            damping_trend=0.8,
            optimized=False
        )

        print("\nModel parameters:")
        print(" - alpha:", holt.params["smoothing_level"])
        print(" - beta :", holt.params["smoothing_trend"])
        print(" - phi  :", holt.params["damping_trend"])

        # --------------------------------------------------
        # FORECAST NEXT YEAR
        # --------------------------------------------------
        forecast = holt.forecast(12)

        print("\nForecast summary:")
        print(" - Mean:", round(forecast.mean(), 2))
        print(" - Min :", round(forecast.min(), 2))
        print(" - Max :", round(forecast.max(), 2))

        # --------------------------------------------------
        # HOLD-OUT EVALUATION
        # --------------------------------------------------
        y_pred_test = holt.forecast(len(y_test))

        mae = mean_absolute_error(y_test, y_pred_test)
        rmse = np.sqrt(mean_squared_error(y_test, y_pred_test))

        print("\nEvaluation on hold-out year:")
        print(" - MAE :", round(mae, 2))
        print(" - RMSE:", round(rmse, 2))

        print("\nInterpretation of evaluation metrics:")
        print(
            f"- MAE = {round(mae, 2)} indicates the average monthly deviation "
            f"between predicted and observed salary values."
        )
        print(
            f"- RMSE = {round(rmse, 2)} penalizes larger errors and reflects "
            f"the typical magnitude of prediction deviations."
        )
        print(
            "- The model focuses on extracting long-term trend rather than "
            "short-term point accuracy."
        )

        # --------------------------------------------------
        # BUILD DECISION BLOCK (DEPEND ON STEP 2)
        # --------------------------------------------------
        if enrich_status == "❌ FAIL":
            decision_block = f"""
        Decision:
        ❌ External trend could NOT be reliably learned.

        Reason:
        - {enrich_reason}

        Note:
        - Model was trained successfully.
        - Forecast output is for inspection only.
        """.strip()

        elif enrich_status == "⚠️ WEAK":
            decision_block = f"""
        Decision:
        ⚠️ External trend learned with WEAK temporal signal.

        Reason:
        - {enrich_reason}

        Note:
        - Forecasting results may be unstable.
        - Use with caution.
        """.strip()

        else:
            decision_block = """
        Decision:
        ✔ External trend successfully learned.
        ✔ Proceed to internal anchoring & final alignment.
        """.strip()


        report = f"""
        === STEP 4: FORECASTING MODEL TRAINING (HOLT) ===

        Purpose:
        - Learn long-term salary trend from external monthly data.
        - Use a time-series model suitable for short-horizon forecasting.

        Training years: {sorted(y_train.index.year.unique())}
        Test year     : {last_year}
        Train months  : {len(y_train)}
        Test months   : {len(y_test)}

        Model parameters:
        - alpha: {holt.params["smoothing_level"]}
        - beta : {holt.params["smoothing_trend"]}
        - phi  : {holt.params["damping_trend"]}

        Forecast summary:
        - Mean: {round(forecast.mean(), 2)}
        - Min : {round(forecast.min(), 2)}
        - Max : {round(forecast.max(), 2)}

        Evaluation on hold-out year:
        - MAE : {round(mae, 2)}
        - RMSE: {round(rmse, 2)}

        Interpretation:
        - Holt model captures level and trend while damping extrapolation.
        - Forecast represents trend-consistent projection, not point accuracy.

        {decision_block}
        """.strip()


        # --------------------------------------------------
        # CHART – HISTORICAL + FORECAST
        # --------------------------------------------------
        fig, ax = plt.subplots(figsize=(10, 4))
        ax.plot(y_series.index, y_series, label="Historical salary")
        ax.plot(forecast.index, forecast, linestyle="--", label="Forecast")
        ax.axvline(forecast.index.min(), linestyle=":", label="Forecast start")
        ax.legend()
        ax.set_title("Salary Trend and Forecast (Holt)")
        ax.grid(True)
        plt.tight_layout()

        # --------------------------------------------------
        # RETURN (TIME-BASED FORECAST PAYLOAD)
        # --------------------------------------------------
        df_forecast = forecast.reset_index()
        df_forecast.columns = ["date", "forecast_salary"]

        payload = {
            "granularity": "monthly",
            "forecast": df_forecast,
        }

        return payload, fig, report

    
    def step_5_internal_anchoring(
        self,
        raw_files: list[Path],
        external_trend: dict,
        time_granularity: str,
    ):

        results = []
        figures = []

        for raw_path in raw_files:
            df_raw = pd.read_csv(raw_path)

            # --------------------------------------------------
            # BUILD INTERNAL MONTHLY FROM RAW FILE
            # --------------------------------------------------
            required_cols = {"posted_date", "min_salary", "max_salary"}
            if not required_cols.issubset(df_raw.columns):
                raise RuntimeError(
                    f"{raw_path.name} missing required columns: "
                    "posted_date, min_salary, max_salary"
                )

            df_int = df_raw.copy()

            df_int["posted_date"] = pd.to_datetime(
                df_int["posted_date"],
                errors="coerce"
            )
            df_int = df_int.dropna(subset=["posted_date"])

            df_int["year_month"] = (
                df_int["posted_date"]
                .dt.to_period("M")
                .dt.to_timestamp()
            )

            df_int["avg_salary"] = (
                df_int[["min_salary", "max_salary"]]
                .mean(axis=1)
            )
            df_int = df_int.dropna(subset=["avg_salary"])

            df_monthly = (
                df_int
                .groupby("year_month")
                .agg(avg_salary=("avg_salary", "mean"))
                .reset_index()
            )

            df_monthly["year"] = df_monthly["year_month"].dt.year

            # --------------------------------------------------
            # SELECT INTERNAL ANCHOR YEAR
            # --------------------------------------------------
            anchor_year = df_monthly["year"].max()
            df_anchor = df_monthly[df_monthly["year"] == anchor_year]
            internal_anchor_avg = df_anchor["avg_salary"].mean()

            # --------------------------------------------------
            # EXTERNAL REFERENCE (TIME-BASED FORECAST FROM STEP 4)
            # --------------------------------------------------
            df_forecast = external_trend["forecast"].copy()

            if time_granularity == "monthly":
                external_anchor_avg = df_forecast["forecast_salary"].mean()

            elif time_granularity == "quarterly":
                external_anchor_avg = (
                    df_forecast
                    .set_index("date")
                    .resample("Q")["forecast_salary"]
                    .mean()
                    .mean()
                )

            elif time_granularity == "yearly":
                external_anchor_avg = (
                    df_forecast
                    .set_index("date")
                    .resample("Y")["forecast_salary"]
                    .mean()
                    .mean()
                )

            else:
                raise RuntimeError(
                    f"Unsupported time granularity: {time_granularity}"
                )

            # --------------------------------------------------
            # COMPUTE ADJUSTMENT FACTOR
            # --------------------------------------------------
            adjustment_factor = internal_anchor_avg / external_anchor_avg

            # --------------------------------------------------
            # APPLY ANCHORING TO FORECAST (TIME-BASED)
            # --------------------------------------------------
            df_final = df_forecast.copy()
            df_final["final_salary"] = (
                df_final["forecast_salary"] * adjustment_factor
            )

            # --------------------------------------------------
            # METADATA
            # --------------------------------------------------
            df_final["anchoring_year"] = anchor_year
            df_final["adjustment_factor"] = adjustment_factor
            df_final["source_file"] = raw_path.name
            df_final["source"] = "external_trend + internal_anchor"

            results.append(df_final)

            # ==================================================
            # CHART 1 – LEVEL COMPARISON (LIKE STEP 9.6)
            # ==================================================
            fig1, ax1 = plt.subplots(figsize=(8, 4))

            levels = [
                internal_anchor_avg,
                external_anchor_avg,
                df_final["final_salary"].mean(),
            ]

            labels = [
                f"Internal actual ({anchor_year})",
                "External trend reference",
                "Final anchored forecast",
            ]

            ax1.bar(labels, levels)
            ax1.set_title("Salary Level Comparison After Anchoring")
            ax1.set_ylabel("Average Salary")
            ax1.grid(axis="y")

            for i, v in enumerate(levels):
                ax1.text(i, v, f"{v:,.0f}", ha="center", va="bottom")

            plt.tight_layout()
            figures.append(fig1)

            # ==================================================
            # CHART 2 – TREND + FINAL ANCHORED FORECAST
            # ==================================================
            fig2, ax2 = plt.subplots(figsize=(10, 4))

            ax2.plot(
                df_forecast["date"],
                df_forecast["forecast_salary"],
                linestyle="--",
                label="External forecast (trend)",
            )

            ax2.plot(
                df_final["date"],
                df_final["final_salary"],
                marker="o",
                label="Final anchored forecast",
            )

            ax2.axvline(
                df_final["date"].min(),
                linestyle=":",
                color="gray",
                label="Anchoring start",
            )

            ax2.set_title("External Trend vs Final Anchored Forecast")
            ax2.set_xlabel("Time")
            ax2.set_ylabel("Salary")
            ax2.legend()
            ax2.grid(True)

            plt.tight_layout()
            figures.append(fig2)

        # --------------------------------------------------
        # CONCAT ALL RESULTS
        # --------------------------------------------------
        df_out = pd.concat(results, ignore_index=True)

        # --------------------------------------------------
        # REPORT (STEP 9.6 STYLE – TIME-BASED)
        # --------------------------------------------------
        report = f"""
    === STEP 5: INTERNAL ANCHORING & FINAL FORECAST ===

    Purpose:
    - Align external salary trend with internal salary reality.
    - Perform LEVEL ADJUSTMENT only.
    - No trend learning or forecasting is performed here.

    Applied source files:
    {chr(10).join(['- ' + p.name for p in raw_files])}

    Anchoring configuration:
    - Time granularity      : {time_granularity}
    - Anchor year (internal): {anchor_year}

    Anchoring statistics:
    - Internal avg salary   : {round(internal_anchor_avg, 2)}
    - External ref avg      : {round(external_anchor_avg, 2)}
    - Adjustment factor     : {round(adjustment_factor, 4)}

    Interpretation:
    - External data provides long-term salary trend shape.
    - Internal data determines realistic salary level.
    - Anchoring adjusts only the LEVEL, not the TREND.

    Decision:
    ✔ Final salary forecast successfully anchored.
    """.strip()

        # NOTE: app hiện chỉ nhận 1 figure → trả chart cuối
        return df_out, figures[-1], report
