#!/usr/bin/env python3
"""Sample test to generate a chart for local testing.

This script will try to call the real `generate_parameter_chart` from
`backend.services.chart_service`. If the DB or environment is not set up it will
fall back to a synthetic dataset and produce a Plotly HTML file at
`./output/sample_chart.html`.

Usage:
  python3 backend/services/sample_chart_test.py

Open the generated HTML in a browser to inspect the chart.
"""
import os
import json
import logging
from datetime import datetime

# Try to import the real chart generator if available
try:
    from backend.services.chart_service import generate_parameter_chart
except Exception as e:
    generate_parameter_chart = None
    logging.warning(f"Could not import generate_parameter_chart: {e}. Falling back to synthetic data.")

# Try Plotly first, fall back to Matplotlib if Plotly is not installed
HAS_PLOTLY = False
HAS_MATPLOTLIB = False
try:
    import plotly.io as pio
    import plotly.graph_objs as go
    HAS_PLOTLY = True
except Exception:
    try:
        import matplotlib.pyplot as plt
        HAS_MATPLOTLIB = True
    except Exception:
        raise RuntimeError("Neither plotly nor matplotlib is available; please install one of them.")


def save_fig(fig, out_path: str):
    """Save a Plotly figure as HTML or a Matplotlib figure as PNG.

    The function will try to save to out_path, and on permission error fall back to
    the user's home directory.
    """
    try:
        os.makedirs(os.path.dirname(out_path), exist_ok=True)
        if HAS_PLOTLY and hasattr(fig, 'write_html'):
            fig.write_html(out_path, include_plotlyjs='cdn')
        elif HAS_MATPLOTLIB:
            # Matplotlib figure expected
            fig.savefig(out_path, bbox_inches='tight')
        else:
            raise RuntimeError('No supported plotting library available')
        print(f"Saved chart to: {out_path}")
    except PermissionError:
        fallback = os.path.join(os.path.expanduser('~'), os.path.basename(out_path))
        try:
            if HAS_PLOTLY and hasattr(fig, 'write_html'):
                fig.write_html(fallback, include_plotlyjs='cdn')
            elif HAS_MATPLOTLIB:
                fig.savefig(fallback, bbox_inches='tight')
            print(f"Permission denied writing to {out_path}. Saved to: {fallback}")
        except Exception as e:
            print(f"Failed to save fallback chart to {fallback}: {e}")
            raise
    except Exception as e:
        print(f"Failed to save chart to {out_path}: {e}")
        raise


def fallback_synthetic_chart(company_number: int, parameters: list, start_year=2018, end_year=2020):
    # Build x-axis labels matching chart_service style: Mar YYYY
    years = list(range(start_year, end_year + 1))
    x_labels = [f"Mar {y}" for y in years]

    # Create some deterministic synthetic values so chart is stable
    base_vals = {
        'Other Non Current Liabilities': 5000,
        'Total Assets': 15000,
        'Total Cash And Short Term Investments': 2000,
    }

    traces = []
    for param in parameters:
        base = base_vals.get(param, 1000)
        # simple growth + variation
        y = [float(base * (1 + 0.05 * (i))) for i, _ in enumerate(years)]
        trace = go.Scatter(x=x_labels, y=y, mode='lines+markers', name=f"Company {company_number} - {param}")
        traces.append(trace)

    fig = go.Figure(data=traces)
    fig.update_layout(title=f"Synthetic Financial Parameters - Company {company_number}", xaxis_title="Year", yaxis_title="Value")
    return fig


def main():
    company_number = 90
    parameters = [
        "Other Non Current Liabilities",
        "Total Assets",
        "Total Cash And Short Term Investments",
    ]

    out_html = os.path.join(os.getcwd(), 'output', 'sample_chart.html')

    # First try to call your real generator (may fail if DB or dependencies missing)
    if generate_parameter_chart is not None:
        try:
            print("Attempting to generate chart using backend.services.chart_service.generate_parameter_chart...")
            res = generate_parameter_chart(company_numbers=[company_number], parameters=parameters, start_year=2018, end_year=2020)
            # The real function returns a dict with 'plotly_json' key (or warning)
            plotly_json = res.get('plotly_json') if isinstance(res, dict) else None
            if plotly_json and plotly_json.strip() and plotly_json.strip() != '{}':
                fig = pio.from_json(plotly_json)
                save_fig(fig, out_html)
                return
            else:
                logging.warning(f"generate_parameter_chart returned no usable plotly_json: {res}")
        except Exception as e:
            logging.exception(f"generate_parameter_chart failed: {e}")

    # Fallback: synthetic chart
    print("Using synthetic fallback chart (no DB or generator unavailable).")
    fig = fallback_synthetic_chart(company_number, parameters, start_year=2018, end_year=2020)
    save_fig(fig, out_html)


if __name__ == '__main__':
    main()

