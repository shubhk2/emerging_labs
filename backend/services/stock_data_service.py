import plotly.graph_objects as go
import plotly.io as pio
# Assuming backend.db_setup and datetime imports are handled correctly elsewhere
from backend.db_setup import connect_to_db

from datetime import datetime, timedelta

# Only allow these three as selectable types
DATA_TYPE_TABLE_MAP = {
    "price": "stock_price",
    "dma50": "stock_dma50",
    "dma200": "stock_dma200",
    # "volume" remains defined but is only used as a complementary trace
    "volume": "stock_volume"
}

PERIOD_MONTHS = {
    "1month": 1,
    "6month": 6,
    "1yr": 12,
    "3yr": 36,
    "5yr": 60,
    "10yr": 120
}


def get_period_start_date(period: str, end_date: str = None):
    if period not in PERIOD_MONTHS:
        raise ValueError("Invalid period")
    months = PERIOD_MONTHS[period]
    if end_date:
        end = datetime.strptime(end_date, "%Y-%m-%d")
    else:
        end = datetime.today()

    # Calculate target year and month
    target_year = end.year
    target_month = end.month - months

    # Adjust year and month if target_month goes below 1
    while target_month <= 0:
        target_month += 12
        target_year -= 1

    try:
        start = end.replace(year=target_year, month=target_month)
    except ValueError:
        # Handle cases like end.day=31 but target_month has only 30 days
        start = end.replace(year=target_year, month=target_month, day=1)
        # Further adjust to the last day of the target month if initial day was too high
        if start.day != end.day and end.day > 28:  # Only needed for days > 28
            last_day_of_month = (start.replace(month=start.month % 12 + 1, day=1) - timedelta(days=1)).day
            start = start.replace(day=min(end.day, last_day_of_month))

    return start.date(), end.date()


# Placeholder for connect_to_db for independent testing



def fetch_stock_data(company_number: int, data_type: str, period: str):
    if data_type not in DATA_TYPE_TABLE_MAP:
        return [], "Invalid data type"
    table = DATA_TYPE_TABLE_MAP[data_type]
    conn = connect_to_db()
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                f"SELECT MAX(date) FROM {table} WHERE company_number = %s",
                (company_number,)
            )
            latest_date = cursor.fetchone()[0]
            if not latest_date:
                return [], "No data available"
            start_date, end_date = get_period_start_date(period, latest_date.strftime("%Y-%m-%d"))
            cursor.execute(
                f"SELECT date, value FROM {table} WHERE company_number = %s AND date >= %s AND date <= %s ORDER BY date ASC",
                (company_number, start_date, end_date)
            )
            rows = cursor.fetchall()
            return rows, None
    finally:
        conn.close()


def create_stock_chart(company_number: int, data_type: str, period: str):
    # Validate selected data_type
    if data_type not in {"price", "dma50", "dma200"}:
        return {"error": "Invalid data type. Choose one of: price, dma50, dma200"}

    rows, error = fetch_stock_data(company_number, data_type, period)
    if error:
        return {"error": error}
    if not rows:
        return {"error": "No data found for the given parameters."}
    main_dates = [row[0].strftime("%Y-%m-%d") for row in rows]
    main_values = [float(row[1]) for row in rows]

    # Always fetch volume data as complementary (used on the right y-axis)
    vol_rows, vol_error = fetch_stock_data(company_number, "volume", period)
    volume_trace = None
    if not vol_error and vol_rows:
        vol_dates = [row[0].strftime("%Y-%m-%d") for row in vol_rows]
        vol_values = [float(row[1]) for row in vol_rows]
        volume_trace = go.Bar(
            x=vol_dates,
            y=vol_values,
            name="Volume",
            yaxis='y2',  # Assign this trace to yaxis2
            marker_color='lightblue',
            opacity=0.7
        )

    fig = go.Figure()
    # Main trace on the left y-axis
    fig.add_trace(
        go.Scatter(
            x=main_dates,
            y=main_values,
            mode='lines+markers',
            name=data_type.capitalize()+'Rs.',
            line=dict(color='blue')
        )
    )
    if volume_trace:
        fig.add_trace(volume_trace)

    # Specify layout with two y-axes
    fig.update_layout(
        xaxis=dict(
            domain=[0.1, 0.9],
            rangeslider_visible=True
        ),
        yaxis=dict(
            title=dict(  # <--- CORRECTED: title is a dict
                text=data_type.capitalize(),
                font=dict(color='blue')  # <--- CORRECTED: font is inside title
            ),
            tickfont=dict(color='blue'),
            side='left'
        ),
        yaxis2=dict(
            title=dict(  # <--- CORRECTED: title is a dict
                text="Volume",
                font=dict(color='lightblue')  # <--- CORRECTED: font is inside title
            ),
            tickfont=dict(color='lightblue'),
            overlaying='y',
            side='right'
        ),
        title=f"{data_type.capitalize()} Chart with Volume",
        legend=dict(
            x=0,
            y=1.1,
            bgcolor='rgba(255, 255, 255, 0)',
            bordercolor='rgba(255, 255, 255, 0)'
        ),
        hovermode='x unified',
        template='plotly_white'
    )
    chart_json = pio.to_json(fig)
    return {"plotly_json": chart_json}


def get_stock_data_table(company_number: int, data_type: str, period: str):
    if data_type not in {"price", "dma50", "dma200"}:
        return {"error": "Invalid data type. Choose one of: price, dma50, dma200"}

    main_rows, main_error = fetch_stock_data(company_number, data_type, period)
    if main_error:
        return {"error": main_error}

    vol_rows, vol_error = fetch_stock_data(company_number, "volume", period)
    table = {
        "main": {
            "columns": ["date", "value"],
            "rows": [[row[0].strftime("%Y-%m-%d"), float(row[1])] for row in main_rows]
        },
        "volume": {
            "columns": ["date", "value"],
            "rows": [[row[0].strftime("%Y-%m-%d"), float(row[1])] for row in vol_rows] if not vol_error else []
        }
    }
    return table


if __name__ == "__main__":
    company_number = 1
    data_type = "price"
    period = "1month"
    chart_result = create_stock_chart(company_number, data_type, period)
    print(chart_result)
    table_result = get_stock_data_table(company_number, data_type, period)
    print(table_result)