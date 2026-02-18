import marimo

__generated_with = "0.19.11"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import altair as alt
    import ibis
    import pandas as pd

    return mo, alt, ibis, pd


@app.cell
def _(mo):
    mo.md("""
    ## NYC Taxi: payment method overview

    This notebook visualizes **taxi trip payment data** with:
    - a **pie chart** (share of Credit / Cash / Other)
    """)
    return


@app.cell
def _(ibis, pd):
    def load_payment_dataframe(max_rows: int) -> tuple[pd.DataFrame, str]:
        """
        Returns (df, source_label).

        df columns:
        - pickup_dt: datetime64[ns]
        - payment_raw: original payment type string
        """
        df_out: pd.DataFrame | None = None
        source_label: str | None = None

        # 1) Try to connect to local DuckDB created by the pipeline.
        try:
            con = ibis.duckdb.connect("taxi_pipeline.duckdb")

            # dlt writes data into a schema (dataset). Find the schema that has taxi_data.
            schemas = con.list_databases()
            taxi_schemas = []
            for s in schemas:
                try:
                    if "taxi_data" in con.list_tables(database=s):
                        taxi_schemas.append(s)
                except Exception:
                    continue

            if taxi_schemas:
                schema = sorted(taxi_schemas)[-1]
                t = con.table("taxi_data", database=schema)

                # Use ibis for data access; bring only needed columns to pandas.
                expr = (
                    t.select(
                        pickup_dt=t.trip_pickup_date_time,
                        payment_raw=t.payment_type,
                    )
                    .limit(int(max_rows))
                )
                df_out = expr.to_pandas()
                df_out["pickup_dt"] = pd.to_datetime(df_out["pickup_dt"], errors="coerce")
                df_out["payment_raw"] = df_out["payment_raw"].astype("string")
                source_label = f"duckdb:{schema}.taxi_data"
        except Exception:
            pass

        # 2) Sample data fallback (demonstration)
        if df_out is None or source_label is None:
            rng = pd.date_range("2009-01-01", periods=180, freq="D")
            df_out = pd.DataFrame(
                {
                    "pickup_dt": rng.repeat(50),
                    "payment_raw": pd.Series(
                        ["Credit"] * 70 + ["Cash"] * 20 + ["No Charge"] * 5 + ["Dispute"] * 5
                    )
                    .sample(len(rng) * 50, replace=True, random_state=7)
                    .reset_index(drop=True),
                }
            )
            source_label = "sample"

        # #region agent log
        try:
            import json, time

            payload = {
                "sessionId": "be1dc8",
                "runId": "initial",
                "hypothesisId": "H1",
                "location": "taxi_payment_marimo.py:load_payment_dataframe",
                "message": "loaded payment dataframe",
                "data": {
                    "source": source_label,
                    "rows": int(len(df_out)) if df_out is not None else 0,
                },
                "timestamp": int(time.time() * 1000),
            }
            with open("debug-be1dc8.log", "a", encoding="utf-8") as f:
                f.write(json.dumps(payload) + "\n")
        except Exception:
            pass
        # #endregion agent log

        return df_out, source_label

    return load_payment_dataframe


@app.cell
def _(max_rows, mo, load_payment_dataframe):
    df, source_label = load_payment_dataframe(int(10000))
    info = mo.md(
        f"**Data source**: `{source_label}`  \n**Rows in memory**: `{len(df):,}`"
    )
    return df, source_label, info


@app.cell
def _(pd):
    def normalize_payment_method(payment_raw: pd.Series) -> pd.Series:
        s = payment_raw.astype("string").fillna("Unknown").str.strip().str.lower()
        return pd.Series(
            pd.NA if s is None else s, index=payment_raw.index, dtype="string"
        ).fillna(s).map(
            lambda v: (
                "Credit"
                if "credit" in v
                else ("Cash" if "cash" in v else ("Other" if v not in ("", "unknown") else "Other"))
            )
        )

    return normalize_payment_method


@app.cell
def _(df, normalize_payment_method):
    data = df.copy()
    data = data.dropna(subset=["pickup_dt"])
    data["payment_method"] = normalize_payment_method(data["payment_raw"])

    # Pie chart summary
    pie_df = (
        data["payment_method"]
        .value_counts(dropna=False)
        .rename_axis("payment_method")
        .reset_index(name="trips")
    )
    pie_df["pct"] = pie_df["trips"] / pie_df["trips"].sum()

    return pie_df


@app.cell
def _(alt, pie_df):
    color_scale = alt.Scale(
        domain=["Credit", "Cash", "Other"],
        range=["#4C78A8", "#F58518", "#54A24B"],
    )

    pie = (
        alt.Chart(pie_df, title="Payment method share")
        .mark_arc(innerRadius=50)
        .encode(
            theta=alt.Theta("trips:Q", stack=True),
            color=alt.Color("payment_method:N", scale=color_scale, title="Payment method"),
            tooltip=[
                alt.Tooltip("payment_method:N", title="Method"),
                alt.Tooltip("trips:Q", title="Trips", format=","),
                alt.Tooltip("pct:Q", title="Share", format=".1%"),
            ],
        )
        .properties(width=360, height=260)
    )

    labels = (
        alt.Chart(pie_df)
        .mark_text(radius=120, size=12)
        .encode(
            theta=alt.Theta("trips:Q", stack=True),
            text=alt.Text("pct:Q", format=".0%"),
        )
    )

    pie_chart = pie + labels
    return pie_chart


@app.cell
def _(line, mo, pie_chart):
    mo.vstack(
        [
            mo.md("### Payment breakdown"),
            pie_chart
        ],
        gap=1.0,
    )
    return