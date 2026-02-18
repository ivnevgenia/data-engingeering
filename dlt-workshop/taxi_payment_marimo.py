import marimo

__generated_with = "0.19.11"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import altair as alt
    import ibis
    import pandas as pd
    import numpy as np
    return mo, alt, ibis, pd, np


@app.cell
def _(mo):
    mo.md("""
    ## NYC Taxi: payment method overview
    - **pie chart** (share of Credit / Cash / Other)
    - **daily revenue chart** (total payments per day)
    - **daily tips chart** (total tips per day)
    """)
    return


@app.cell
def _(ibis, pd):
    def load_payment_dataframe(max_rows: int) -> tuple[pd.DataFrame, str]:
        """Load taxi data from DuckDB or return empty DataFrame"""
        df_out = None
        source_label = None

        try:
            con = ibis.duckdb.connect("taxi_pipeline.duckdb")
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
                
                total_rows = t.count().execute()
                print(f"Total rows: {total_rows}")
                
                if total_rows > max_rows:
                    expr = t.select([
                        t.trip_pickup_date_time,
                        t.payment_type,
                        t.total_amt,
                        t.tip_amt
                    ]).sample(fraction=max_rows/total_rows)
                else:
                    expr = t.select([
                        t.trip_pickup_date_time,
                        t.payment_type,
                        t.total_amt,
                        t.tip_amt
                    ])
                
                df_out = expr.to_pandas().rename(columns={
                    'trip_pickup_date_time': 'pickup_dt',
                    'payment_type': 'payment_raw',
                    'total_amt': 'total_amt',
                    'tip_amt': 'tip_amt'
                })
                
                df_out["pickup_dt"] = pd.to_datetime(df_out["pickup_dt"], errors="coerce")
                df_out["payment_raw"] = df_out["payment_raw"].astype("string")
                df_out["tip_amt"] = pd.to_numeric(df_out["tip_amt"], errors="coerce")
                df_out = df_out.dropna(subset=["pickup_dt", "total_amt"])
                
                source_label = f"duckdb:{schema}.taxi_data"
                print(f"Loaded {len(df_out)} rows")
                
        except Exception as e:
            print(f"DB error: {e}")

        if df_out is None or len(df_out) == 0:
            df_out = pd.DataFrame(columns=["pickup_dt", "payment_raw", "total_amt", "tip_amt"])
            source_label = "no_data"

        return df_out, source_label

    return load_payment_dataframe


@app.cell
def _(mo, load_payment_dataframe):
    df, source_label = load_payment_dataframe(50000)
    
    if len(df) > 0:
        info = mo.md(
            f"**Source**: `{source_label}`  \n"
            f"**Rows**: `{len(df):,}`  \n"
            f"**Dates**: `{df['pickup_dt'].min()} to {df['pickup_dt'].max()}`  \n"
            f"**Total tips**: `${df['tip_amt'].sum():,.2f}`  \n"
            f"**Trips with tips**: `{(df['tip_amt'] > 0).sum():,}`"
        )
    else:
        info = mo.md(f"**Source**: `{source_label}` - No data")
    
    return df, source_label, info


@app.cell
def _(pd):
    def normalize_payment_method(payment_raw: pd.Series) -> pd.Series:
        """Convert payment_type to Credit/Cash/Other"""
        s = payment_raw.astype("string").fillna("").str.strip().str.lower()
        
        def map_payment(v):
            if pd.isna(v) or v == "":
                return "Other"
            v_str = str(v).lower()
            
            if any(x in v_str for x in ['credit', 'card']):
                return "Credit"
            elif any(x in v_str for x in ['cash']):
                return "Cash"
            else:
                try:
                    code = int(float(v_str))
                    if code == 1:
                        return "Credit"
                    elif code == 2:
                        return "Cash"
                    else:
                        return "Other"
                except:
                    return "Other"
        
        return s.map(map_payment)

    return normalize_payment_method


@app.cell
def _(df, normalize_payment_method):
    if len(df) > 0:
        data = df.copy()
        data["tip_amt"] = pd.to_numeric(data["tip_amt"], errors="coerce").fillna(0)
        data["payment_method"] = normalize_payment_method(data["payment_raw"])
        data["trip_date"] = data["pickup_dt"].dt.date

        # Pie chart data
        pie_df = data["payment_method"].value_counts().rename_axis("payment_method").reset_index(name="trips")
        pie_df["pct"] = pie_df["trips"] / pie_df["trips"].sum()

        # Daily aggregates
        daily_df = data.groupby("trip_date", as_index=False).agg({
            "total_amt": "sum",
            "tip_amt": "sum",
            "payment_method": "count"
        }).rename(columns={"payment_method": "trip_count"}).sort_values("trip_date")
        
        daily_df["trip_date"] = pd.to_datetime(daily_df["trip_date"])
        daily_df["avg_tip_per_trip"] = daily_df["tip_amt"] / daily_df["trip_count"]
        daily_df["tip_percentage"] = daily_df.apply(
            lambda r: (r["tip_amt"] / r["total_amt"] * 100) if r["total_amt"] > 0 else 0, axis=1
        )

        print(f"Days: {len(daily_df)}, Tips total: ${daily_df['tip_amt'].sum():,.2f}")
    else:
        data = pie_df = daily_df = pd.DataFrame()

    return data, daily_df, pie_df


@app.cell
def _(alt, pie_df):
    if len(pie_df) > 0:
        color_scale = alt.Scale(domain=["Credit", "Cash", "Other"], range=["#4C78A8", "#F58518", "#54A24B"])

        pie = alt.Chart(pie_df, title="Payment method share").mark_arc(innerRadius=50).encode(
            theta=alt.Theta("trips:Q", stack=True),
            color=alt.Color("payment_method:N", scale=color_scale),
            tooltip=["payment_method:N", "trips:Q", alt.Tooltip("pct:Q", format=".1%")]
        ).properties(width=360, height=260)

        labels = alt.Chart(pie_df).mark_text(radius=120, size=12).encode(
            theta=alt.Theta("trips:Q", stack=True),
            text=alt.Text("pct:Q", format=".0%")
        )

        pie_chart = pie + labels
    else:
        pie_chart = alt.Chart().mark_text(text="No data").properties(width=360, height=260)
    return pie_chart


@app.cell
def _(alt, daily_df):
    if len(daily_df) > 0 and daily_df['total_amt'].sum() > 0:
        # Moving averages
        daily_df['revenue_ma7'] = daily_df['total_amt'].rolling(7, min_periods=1).mean()
        daily_df['tips_ma7'] = daily_df['tip_amt'].rolling(7, min_periods=1).mean()

        # Revenue chart
        revenue_base = alt.Chart(daily_df).encode(
            x=alt.X("trip_date:T", title="Date", axis=alt.Axis(format="%Y-%m-%d", labelAngle=-45))
        ).properties(width=700, height=300)

        revenue_chart = (
            revenue_base.mark_line(color="#4C78A8", strokeWidth=2) +
            revenue_base.mark_point(color="#4C78A8", size=40, opacity=0.7) +
            revenue_base.mark_line(color="red", strokeWidth=1.5, opacity=0.7).encode(y="revenue_ma7:Q")
        ).encode(
            y=alt.Y("total_amt:Q", title="Total ($)", axis=alt.Axis(format="$,.0f")),
            tooltip=["trip_date:T", alt.Tooltip("total_amt:Q", format="$,.2f"), "trip_count:Q"]
        ).properties(title="Daily Revenue")

        # Tips chart
        tips_chart = (
            revenue_base.mark_line(color="#F58518", strokeWidth=2) +
            revenue_base.mark_circle(color="#F58518", size=100, opacity=1, stroke='black', strokeWidth=1) +
            revenue_base.mark_line(color="red", strokeWidth=1.5, opacity=0.7).encode(y="tips_ma7:Q")
        ).encode(
            y=alt.Y("tip_amt:Q", title="Tips ($)", axis=alt.Axis(format="$,.2f")),
            tooltip=[
                "trip_date:T",
                alt.Tooltip("tip_amt:Q", format="$,.2f"),
                alt.Tooltip("avg_tip_per_trip:Q", format="$,.2f"),
                alt.Tooltip("tip_percentage:Q", format=".1f"),
                "trip_count:Q"
            ]
        ).properties(title="Daily Tips")

        date_range = f"{daily_df['trip_date'].min().date()} to {daily_df['trip_date'].max().date()}"
    else:
        revenue_chart = alt.Chart().mark_text(text="No data").properties(width=700, height=300)
        tips_chart = alt.Chart().mark_text(text="No data").properties(width=700, height=300)
        date_range = "No data"

    return date_range, revenue_chart, tips_chart


@app.cell
def _(mo, pie_chart, revenue_chart, tips_chart, info, date_range):
    mo.vstack([
        info,
        mo.md("### Payment breakdown"),
        pie_chart,
        mo.md(f"### Daily revenue and tips ({date_range})"),
        revenue_chart,
        tips_chart,
    ], gap=1.0)
    return
