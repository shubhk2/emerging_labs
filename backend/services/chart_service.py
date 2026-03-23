from backend.db_setup import connect_to_db
from psycopg2.extras import RealDictCursor
from typing import List, Optional, Dict, Any
import logging
import plotly.graph_objs as go
import plotly.io as pio

logger = logging.getLogger(__name__)

# Parameter lists converted to lowercase sets for efficient, case-insensitive lookup
bs_parameters = {
    "finance division loans and leases long-term", "long-term debt", "net property plant and equipment",
    "current income taxes payable", "deferred tax assets long-term (collected)", "gross property plant and equipment",
    "non-interest bearing deposits", "total receivables", "current portion of leases", "gross loans",
    "separate account liability", "current portion of long-term debt", "common stock, total",
    "investment securities, total", "short-term borrowings", "unpaid claims", "insurance and annuity liabilities",
    "unearned revenue non current", "total other investments", "accrued interest payable",
    "accounts receivable long-term", "other intangibles, total", "deferred charges long-term", "total current assets",
    "total investments", "comprehensive income and other", "treasury stock", "net loans",
    "deferred tax assets long-term", "prepaid expenses", "unearned revenue current, total", "total common equity",
    "inventory", "interest bearing deposits", "preferred stock non redeemable", "unearned revenue, current",
    "policy loans", "mortgage loans", "additional paid in capital", "finance division loans and leases current",
    "total liabilities and equity", "accumulated depreciation", "deferred tax liability non-current",
    "minority interest", "investment in debt securities", "allowance for loan losses",
    "other real estate owned and foreclosed", "total liabilities", "total preferred equity", "long-term investments",
    "other current assets, total", "real estate owned", "separate account assets", "reinsurance recoverable",
    "preferred stock redeemable", "accrued interest receivable", "long-term leases", "accounts receivable, total",
    "other receivables", "total assets", "loans receivable long-term", "preferred stock convertible",
    "accrued expenses, total", "notes receivable", "other long-term assets, total", "other current liabilities",
    "accounts payable, total", "total current liabilities", "other non current liabilities", "goodwill",
    "cash and equivalents", "short term investments", "total deposits", "pension & other post retirement benefits",
    "unearned premiums", "investment in equity and preferred securities, total", "restricted cash",
    "retained earnings", "reinsurance payable", "total cash and short term investments",
    "trading asset securities, total", "total equity"
}
pl_parameters = {
    "total interest and dividend income", "gain (loss) on sale of loans", "non-insurance activities revenues",
    "net income", "gain (loss) on sale of investment, total", "preferred dividend and other adjustments",
    "interest income on investments", "cost of goods sold, total", "depreciation & amortization - (collected)",
    "restructuring charges", "other operating expenses, total", "gain (loss) on sale of invest. & securities",
    "total revenues", "ebt, excl. unusual items", "insurance division revenues", "r&d expenses",
    "interest and invest. income", "legal settlements", "earnings of discontinued operations", "income tax expense",
    "policy acquisition / underwriting costs, total", "total operating expenses", "impairment of goodwill",
    "non interest expense, total", "interest expense, total", "credit card fee", "other revenues, total",
    "net interest income", "extraordinary item & accounting change", "selling general & admin expenses, total",
    "provision for bad debts", "total shares outstanding", "operating revenues", "minority interest",
    "earnings from continuing operations", "exploration / drilling costs, total",
    "other non operating income (expenses)",
    "depreciation & amortization", "non interest income, total", "weighted avg. shares outstanding", "gross profit",
    "impairment of oil, gas & mineral properties", "ebitda", "(income) loss on equity invest.", "operating margin",
    "total other non interest income", "revenues before provison for loan losses", "income (loss) on equity invest.",
    "weighted avg. shares outstanding dil", "effective tax rate", "other unusual items",
    "gain (loss) on sale of assets",
    "interest expense - finance division", "gross profit margin", "finance div. revenues", "stock-based compensation",
    "eps diluted", "net income to common excl. extra items", "gain (loss) on sale of investments",
    "interest income on loans", "operating income", "net interest expenses", "insurance settlements",
    "ebt, incl. unusual items", "currency exchange gains (loss)", "total revenues % chg.",
    "total other non interest expense", "net income to common incl extra items",
    "merger & related restructuring charges",
    "premiums and annuity revenues", "provision for loan losses", "total merger & related restructuring charges",
    "asset writedown", "salaries and other employee benefits", "policy benefits", "other operating expenses",
    "interest income, total", "occupancy expense", "interest on deposits", "eps", "interest and investment income"
}
cf_parameters = {
    "amortization of goodwill and intangible assets", "change in accounts receivable",
    "sale (purchase) of real estate properties", "investment in marketable and equity securities, total", "net income",
    "special dividend paid", "asset writedown & restructuring costs", "depreciation, depletion & amortization",
    "net cash from discontinued operations", "issuance of preferred stock", "other operating activities, total",
    "capital expenditure", "amortization of deferred charges, total", "change in unearned revenues",
    "total depreciation, depletion & amortization", "total debt issued", "(gain) loss on sale of asset",
    "other financing activities, total", "provision and write-off of bad debts", "provision for credit losses",
    "foreign exchange rate adjustments", "depreciation & amortization", "miscellaneous cash flow adjustments",
    "other operating activities", "(gain) loss from sale of asset", "total asset writedown",
    "short term debt repaid, total", "(income) loss on equity investments",
    "impairment of oil, gas & mineral properties",
    "common dividends paid", "long-term debt repaid, total", "total debt repaid", "reinsurance recoverable",
    "(gain) loss on sale of investments", "sale (purchase) of intangible assets", "change in inventories",
    "purchase / sale of intangible assets", "change in other net operating assets (collected)",
    "stock-based compensation", "sale of property, plant and equipment (collected)", "long-term debt issued, total",
    "nopat", "net (increase) decrease in loans originated / sold - investing", "tax benefit from stock options",
    "cash from operations", "divestitures", "net increase (decrease) in deposit accounts",
    "short term debt issued, total", "sale of property, plant, and equipment", "change in other net operating assets",
    "repurchase of preferred stock", "depreciation & amortization, total", "repurchase of common stock",
    "change in insurance reserves / liabilities", "common & preferred stock dividends paid", "cash from financing",
    "issuance of common stock", "cash from investing", "other investing activities, total",
    "change in accounts payable", "preferred dividends paid", "net change in cash", "cash acquisitions"
}

l=list(pl_parameters)+list(bs_parameters)+list(cf_parameters)
print(len(l))

def to_float_for_plotting(value: Any) -> Optional[float]:
    """Safely convert a value to a float for plotting, returning None for invalid inputs."""
    if value is None:
        return None
    try:
        # Handle common non-numeric strings by treating them as no data
        if isinstance(value, str) and value.strip() in ('', '-', '—', 'N/A'):
            return None
        return float(value)
    except (ValueError, TypeError):
        return None


def get_table_for_parameter(parameter: str) -> Optional[str]:
    """Determine which financial statement table to query based on the parameter."""
    logger.debug(f"Determining table for parameter: '{parameter}'")
    param_lower = parameter.lower()
    if param_lower in pl_parameters:
        # print("yes")
        return "profit_and_loss"
    elif param_lower in bs_parameters:
        return "balance_sheet"
    elif param_lower in cf_parameters:
        return "cashflow"
    else:
        logger.warning(f"Parameter '{parameter}' not found in any financial statement.")
        return None


# Python
def generate_parameter_chart(
        company_numbers: List[int],
        parameters: List[str],
        start_year: Optional[int] = None,
        end_year: Optional[int] = None,
        chart_type: str = "line"
) -> Dict[str, Any]:
    """Generate a Plotly chart for specified financial parameters and companies."""
    logger.info(
        f"Generating parameter chart for companies={company_numbers}, parameters={parameters}, years={start_year}-{end_year}, chart_type={chart_type}")
    conn = connect_to_db()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cursor.execute("SELECT id, full_name FROM company_detail WHERE id = ANY(%s)", (company_numbers,))
        company_details_map = {row['id']: row for row in cursor.fetchall()}

        start = start_year - 2000 if start_year else 16
        end = end_year - 2000 if end_year else 25
        selected_year_cols = [f'mar_{yr}' for yr in range(start, end + 1)]
        x_axis_labels = [f"Mar {2000 + int(col.split('_')[1])}" for col in selected_year_cols]

        traces = []
        for company_number in company_numbers:
            company_info = company_details_map.get(company_number)
            if not company_info:
                logger.warning(f"Details not found for company_number {company_number}")
                continue

            for parameter in parameters:
                table_name = get_table_for_parameter(parameter)
                if not table_name:
                    logger.warning(f"Skipping parameter '{parameter}' as its table could not be determined.")
                    continue

                query = f"SELECT {', '.join(selected_year_cols)} FROM public.{table_name} WHERE company_number = %s AND account = %s"
                cursor.execute(query, (company_number, parameter))
                data_row = cursor.fetchone()

                if data_row:
                    y_values = [to_float_for_plotting(data_row[col]) for col in selected_year_cols]
                    if chart_type == "bar":
                        trace = go.Bar(x=x_axis_labels, y=y_values, name=f"{company_info['full_name']} - {parameter}")
                    else:  # Default to line chart
                        trace = go.Scatter(x=x_axis_labels, y=y_values, mode='lines+markers',
                                           name=f"{company_info['full_name']} - {parameter}")
                    traces.append(trace)
                else:
                    logger.warning(f"No data for parameter '{parameter}' for company '{company_info['full_name']}'")

        if not traces:
            return {"plotly_json": "{}", "warning": "No data found for the specified parameters and companies."}

        fig = go.Figure(data=traces)
        fig.update_layout(title="Financial Parameter Comparison", xaxis_title="Year", yaxis_title="Value")
        return {"plotly_json": pio.to_json(fig)}
    finally:
        cursor.close()
        conn.close()


def generate_ratio_chart(
        company_numbers: List[int],
        parameters: List[str],
        start_year: Optional[int] = None,
        end_year: Optional[int] = None,
        chart_type: str = "line"
) -> Dict[str, Any]:
    """Generate a Plotly chart for specified financial ratios and companies."""
    logger.info(
        f"Generating ratio chart for companies={company_numbers}, ratios={parameters}, years={start_year}-{end_year}, chart_type={chart_type}")
    conn = connect_to_db()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cursor.execute("SELECT id, full_name FROM company_detail WHERE id = ANY(%s)", (company_numbers,))
        company_details_map = {row['id']: row for row in cursor.fetchall()}

        start = start_year - 2000 if start_year else 16
        end = end_year - 2000 if end_year else 25
        selected_year_cols = [f'mar_{yr}' for yr in range(start, end + 1)]
        x_axis_labels = [f"Mar {2000 + int(col.split('_')[1])}" for col in selected_year_cols]

        traces = []
        for company_number in company_numbers:
            company_info = company_details_map.get(company_number)
            if not company_info:
                continue

            for ratio_name in parameters:
                query = f"SELECT {', '.join(selected_year_cols)} FROM public.financial_ratios WHERE company_number = %s AND name = %s"
                cursor.execute(query, (company_number, ratio_name))
                data_row = cursor.fetchone()

                if data_row:
                    y_values = [to_float_for_plotting(data_row[col]) for col in selected_year_cols]
                    if chart_type == "bar":
                        trace = go.Bar(x=x_axis_labels, y=y_values, name=f"{company_info['full_name']} - {ratio_name}")
                    else:  # Default to line chart
                        trace = go.Scatter(x=x_axis_labels, y=y_values, mode='lines+markers',
                                           name=f"{company_info['full_name']} - {ratio_name}")
                    traces.append(trace)
                else:
                    logger.warning(f"No data for ratio '{ratio_name}' for company '{company_info['full_name']}'")

        if not traces:
            return {"plotly_json": "{}", "warning": "No data found for the specified ratios and companies."}

        fig = go.Figure(data=traces)
        fig.update_layout(title="Financial Ratio Comparison", xaxis_title="Year", yaxis_title="Value")
        return {"plotly_json": pio.to_json(fig)}
    finally:
        cursor.close()
        conn.close()
if __name__ == "__main__":
    # Example usage
    chart_data = generate_parameter_chart(
        company_numbers=[90, 42],
        parameters=["Gross Profit", "Depreciation & Amortization"],
        start_year=2018,
        end_year=2020
    )
    print(chart_data)

    ratio_data = generate_ratio_chart(
        company_numbers=[90, 42],
        parameters=["debt_to_equity", "cash_ratio"],
        start_year=2018,
        end_year=2020
    )
    print(ratio_data)