"""
Summary routes for DiyurCalc application.
Contains general summary and export functionality.
"""
from __future__ import annotations

import time
from datetime import datetime
from typing import Optional

from fastapi import Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from config import config
from database import get_conn
from logic import (
    get_payment_codes,
    calculate_monthly_summary,
)
from utils import human_date, format_currency
import logging

logger = logging.getLogger(__name__)
templates = Jinja2Templates(directory=str(config.TEMPLATES_DIR))
templates.env.filters["human_date"] = human_date
templates.env.filters["format_currency"] = format_currency


def general_summary(
    request: Request,
    year: Optional[int] = None,
    month: Optional[int] = None
) -> HTMLResponse:
    """General monthly summary view."""
    start_time = time.time()
    logger.info(f"Starting general_summary for {month}/{year}")

    # Set default date if not provided
    now = datetime.now(config.LOCAL_TZ)
    if year is None:
        year = now.year
    if month is None:
        month = now.month

    # חישוב טווח התאריכים לחודש הנבחר (לשליפת רכיבי תשלום)
    month_start = datetime(year, month, 1, tzinfo=config.LOCAL_TZ)
    if month == 12:
        month_end = datetime(year + 1, 1, 1, tzinfo=config.LOCAL_TZ)
    else:
        month_end = datetime(year, month + 1, 1, tzinfo=config.LOCAL_TZ)
    month_start_ts = int(month_start.timestamp())
    month_end_ts = int(month_end.timestamp())

    with get_conn() as conn:
        # 1. Fetch Payment Codes
        payment_codes = get_payment_codes(conn.conn)

        pre_calc_time = time.time()
        logger.info("Starting optimized calculation...")

        # Use optimized bulk calculation
        summary_data, grand_totals = calculate_monthly_summary(conn.conn, year, month)

        loop_time = time.time() - pre_calc_time
        logger.info(f"Optimized calculation took: {loop_time:.4f}s")

    year_options = [2023, 2024, 2025, 2026]
    total_time = time.time() - start_time
    logger.info(f"Total general_summary execution time: {total_time:.4f}s")

    return templates.TemplateResponse("general_summary.html", {
        "request": request,
        "payment_codes": payment_codes,
        "summary_data": summary_data,
        "grand_totals": grand_totals,
        "selected_year": year,
        "selected_month": month,
        "years": year_options
    })