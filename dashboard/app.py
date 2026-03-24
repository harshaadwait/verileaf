"""
VeriLeaf Compliance Dashboard

Streamlit app for viewing and acknowledging compliance discrepancies,
generating government reports, and manually triggering reconciliation.
Connects to the VeriLeaf FastAPI backend.

Usage:
    streamlit run dashboard/app.py
"""
import calendar
from datetime import date, timedelta

import httpx
import pandas as pd
import streamlit as st

st.set_page_config(
    page_title="VeriLeaf — Compliance Dashboard",
    layout="wide",
    initial_sidebar_state="expanded",
)


# ── Sidebar ───────────────────────────────────────────────────────────────────

with st.sidebar:
    st.title("VeriLeaf")
    st.caption("Cannabis Compliance Dashboard")
    st.divider()

    api_base = st.text_input("API Base URL", value="http://localhost:8000").rstrip("/")
    location_id = st.text_input("Location ID", value="LOC-001")

    st.divider()

    if st.button("Refresh Data", use_container_width=True):
        st.cache_data.clear()
        for key in [k for k in st.session_state if k.startswith("report_") or k == "recon_result"]:
            del st.session_state[key]
        st.rerun()


# ── API helpers ───────────────────────────────────────────────────────────────

@st.cache_data(ttl=10, show_spinner=False)
def _is_api_up(base: str) -> bool:
    try:
        return httpx.get(f"{base}/health", timeout=4).status_code == 200
    except Exception:
        return False


@st.cache_data(ttl=30, show_spinner=False)
def _get_discrepancies(base: str, loc: str, acknowledged: bool) -> tuple[list, str | None]:
    try:
        r = httpx.get(
            f"{base}/discrepancies",
            params={"location_id": loc, "acknowledged": str(acknowledged).lower()},
            timeout=10,
        )
        r.raise_for_status()
        return r.json(), None
    except httpx.HTTPStatusError as e:
        msg = e.response.json().get("detail", e.response.text) if e.response.content else str(e)
        return [], f"HTTP {e.response.status_code}: {msg}"
    except Exception as e:
        return [], str(e)


def _acknowledge(base: str, disc_id: str, by: str, notes: str) -> tuple[bool, str | None]:
    try:
        r = httpx.post(
            f"{base}/discrepancies/{disc_id}/acknowledge",
            json={"acknowledged_by": by, "notes": notes},
            timeout=10,
        )
        r.raise_for_status()
        return True, None
    except httpx.HTTPStatusError as e:
        msg = e.response.json().get("detail", e.response.text) if e.response.content else str(e)
        return False, f"HTTP {e.response.status_code}: {msg}"
    except Exception as e:
        return False, str(e)


def _get_report(base: str, loc: str, report_type: str, year: int, month: int) -> tuple[bytes | None, str | None]:
    try:
        r = httpx.get(
            f"{base}/reports/{report_type}",
            params={"location_id": loc, "year": year, "month": month},
            timeout=30,
        )
        r.raise_for_status()
        return r.content, None
    except httpx.HTTPStatusError as e:
        msg = e.response.json().get("detail", e.response.text) if e.response.content else str(e)
        return None, f"HTTP {e.response.status_code}: {msg}"
    except Exception as e:
        return None, str(e)


def _reconcile(base: str, loc: str, for_date: date) -> tuple[dict | None, str | None]:
    try:
        r = httpx.post(
            f"{base}/reconcile/{loc}",
            params={"report_date": for_date.isoformat()},
            timeout=60,
        )
        r.raise_for_status()
        return r.json(), None
    except httpx.HTTPStatusError as e:
        msg = e.response.json().get("detail", e.response.text) if e.response.content else str(e)
        return None, f"HTTP {e.response.status_code}: {msg}"
    except Exception as e:
        return None, str(e)


# ── Connection guard ──────────────────────────────────────────────────────────

if not _is_api_up(api_base):
    st.error(f"Cannot reach VeriLeaf API at `{api_base}`")
    st.markdown("Make sure the backend is running:")
    st.code("docker compose up api", language="bash")
    st.stop()


# ── Fetch discrepancies ───────────────────────────────────────────────────────

open_discs, open_err = _get_discrepancies(api_base, location_id, acknowledged=False)
acked_discs, _ = _get_discrepancies(api_base, location_id, acknowledged=True)


# ── Page header ───────────────────────────────────────────────────────────────

st.title("Compliance Dashboard")
st.caption(f"Location: **{location_id}**")

if open_err:
    st.error(f"Error loading discrepancies: {open_err}")
    st.stop()


# ── Metrics row ───────────────────────────────────────────────────────────────

c1, c2, c3, c4 = st.columns(4)

c1.metric(
    "Open Discrepancies",
    len(open_discs),
    delta=f"{len(open_discs)} pending" if open_discs else None,
    delta_color="inverse",
    help="Must all be acknowledged before reports can be generated.",
)
c2.metric("Acknowledged", len(acked_discs or []))
c3.metric(
    "Products Affected",
    len({d["product_id"] for d in open_discs}) if open_discs else 0,
)
c4.metric(
    "Oldest Open Date",
    min(d["report_date"] for d in open_discs) if open_discs else "None",
)

st.divider()


# ── Tabs ──────────────────────────────────────────────────────────────────────

tab_open, tab_acked, tab_reports, tab_recon = st.tabs([
    f"Open Discrepancies ({len(open_discs)})",
    f"Acknowledged ({len(acked_discs or [])})",
    "Reports",
    "Reconciliation",
])


# ─── Tab 1: Open Discrepancies ────────────────────────────────────────────────

with tab_open:
    if not open_discs:
        st.success("No open discrepancies — all reports can be generated freely.")
    else:
        st.warning(
            f"{len(open_discs)} open discrepancy/ies are blocking report generation. "
            "Acknowledge all of them to unlock the Reports tab for affected periods."
        )

        # Discrepancy table
        df_open = pd.DataFrame([
            {
                "ID": d["id"],
                "Date": d["report_date"],
                "Product ID": d["product_id"],
                "Internal (g)": d["internal_qty"],
                "Greenline (g)": d["greenline_qty"],
                "Delta (g)": d["delta"],
            }
            for d in open_discs
        ])
        st.dataframe(
            df_open,
            use_container_width=True,
            hide_index=True,
            column_config={
                "ID": st.column_config.TextColumn(width="medium"),
                "Date": st.column_config.TextColumn(width="small"),
                "Delta (g)": st.column_config.TextColumn(width="small"),
            },
        )

        # Acknowledge form
        st.subheader("Acknowledge a Discrepancy")
        st.markdown(
            "Select the discrepancy, provide your name, and explain the root cause. "
            "This action is logged and cannot be undone."
        )

        options = {
            f"{d['report_date']} — {d['product_id']}  (delta {d['delta']} g)": d["id"]
            for d in open_discs
        }

        with st.form("ack_form", clear_on_submit=True):
            selected = st.selectbox("Discrepancy", options=list(options.keys()))
            ack_by = st.text_input(
                "Your name or email *",
                placeholder="manager@cannabis.ca",
            )
            notes = st.text_area(
                "Notes",
                placeholder=(
                    "Describe the root cause — e.g. physical count confirmed 115 g "
                    "on hand; 5 g attributed to evaporation loss within policy."
                ),
                height=100,
            )
            submitted = st.form_submit_button(
                "Acknowledge Discrepancy",
                type="primary",
                use_container_width=True,
            )

        if submitted:
            if not ack_by.strip():
                st.error("Name / email is required.")
            else:
                with st.spinner("Acknowledging..."):
                    ok, err = _acknowledge(
                        api_base, options[selected], ack_by.strip(), notes.strip()
                    )
                if err:
                    st.error(f"Failed: {err}")
                else:
                    st.success("Discrepancy acknowledged.")
                    st.cache_data.clear()
                    st.rerun()


# ─── Tab 2: Acknowledged ──────────────────────────────────────────────────────

with tab_acked:
    if not acked_discs:
        st.info("No acknowledged discrepancies for this location yet.")
    else:
        df_acked = pd.DataFrame([
            {
                "Date": d["report_date"],
                "Product ID": d["product_id"],
                "Internal (g)": d["internal_qty"],
                "Greenline (g)": d["greenline_qty"],
                "Delta (g)": d["delta"],
                "Acknowledged By": d.get("acknowledged_by") or "",
                "Notes": d.get("notes") or "",
            }
            for d in acked_discs
        ])
        st.dataframe(
            df_acked,
            use_container_width=True,
            hide_index=True,
            column_config={
                "Notes": st.column_config.TextColumn(width="large"),
            },
        )


# ─── Tab 3: Reports ───────────────────────────────────────────────────────────

with tab_reports:
    today = date.today()
    prev = (today.replace(day=1) - timedelta(days=1))  # last day of previous month

    col_yr, col_mo, _ = st.columns([1, 1, 2])
    report_year = int(col_yr.number_input(
        "Year", min_value=2020, max_value=today.year, value=prev.year, step=1
    ))
    report_month = col_mo.selectbox(
        "Month",
        options=list(range(1, 13)),
        index=prev.month - 1,
        format_func=lambda m: calendar.month_name[m],
    )

    # Warn if open discrepancies exist for this period
    period_prefix = f"{report_year}-{report_month:02d}"
    blocking = [d for d in open_discs if d["report_date"].startswith(period_prefix)]
    if blocking:
        st.warning(
            f"{len(blocking)} open discrepancy/ies for "
            f"{calendar.month_name[report_month]} {report_year} will block generation."
        )

    st.divider()

    agco_key = f"report_agco_{report_year}_{report_month}_{location_id}"
    ctls_key = f"report_ctls_{report_year}_{report_month}_{location_id}"

    col_agco, col_ctls = st.columns(2)

    with col_agco:
        st.markdown("#### AGCO Monthly Report")
        st.caption("Ontario Alcohol and Gaming Commission of Ontario retail sales report.")
        if st.button("Generate AGCO CSV", use_container_width=True, key="gen_agco"):
            with st.spinner("Generating AGCO report..."):
                csv_bytes, err = _get_report(api_base, location_id, "agco", report_year, report_month)
            if err:
                st.error(err)
            else:
                st.session_state[agco_key] = csv_bytes

        if agco_key in st.session_state:
            st.download_button(
                "Download AGCO CSV",
                data=st.session_state[agco_key],
                file_name=f"AGCO_{location_id}_{report_year}-{report_month:02d}.csv",
                mime="text/csv",
                use_container_width=True,
                type="primary",
                key="dl_agco",
            )

    with col_ctls:
        st.markdown("#### Health Canada CTLS Submission")
        st.caption("Cannabis Tracking and Licensing System monthly submission.")
        if st.button("Generate CTLS CSV", use_container_width=True, key="gen_ctls"):
            with st.spinner("Generating CTLS report..."):
                csv_bytes, err = _get_report(api_base, location_id, "ctls", report_year, report_month)
            if err:
                st.error(err)
            else:
                st.session_state[ctls_key] = csv_bytes

        if ctls_key in st.session_state:
            st.download_button(
                "Download CTLS CSV",
                data=st.session_state[ctls_key],
                file_name=f"CTLS_{location_id}_{report_year}-{report_month:02d}.csv",
                mime="text/csv",
                use_container_width=True,
                type="primary",
                key="dl_ctls",
            )


# ─── Tab 4: Reconciliation ────────────────────────────────────────────────────

with tab_recon:
    st.markdown(
        "Manually trigger the reconciliation engine for a specific date. "
        "Compares internal event totals against Greenline's live inventory snapshot "
        "and flags any drift outside the ±0.5 g tolerance."
    )
    st.info(
        "In production this runs automatically at 23:59 via Celery Beat. "
        "Use this to re-run a date after correcting events or for ad-hoc audits."
    )

    recon_date = st.date_input(
        "Reconciliation date",
        value=date.today() - timedelta(days=1),
        min_value=date(2020, 1, 1),
        max_value=date.today(),
    )

    if st.button("Run Reconciliation", type="primary"):
        with st.spinner(f"Reconciling {location_id} for {recon_date}..."):
            summary, err = _reconcile(api_base, location_id, recon_date)

        if err:
            st.error(f"Reconciliation failed: {err}")
        else:
            st.session_state["recon_result"] = summary
            st.cache_data.clear()

    if "recon_result" in st.session_state:
        summary = st.session_state["recon_result"]
        st.success(f"Reconciliation complete — {summary['report_date']}")

        mc1, mc2, mc3 = st.columns(3)
        mc1.metric("Total Products", summary["total_products"])
        mc2.metric("Reconciled", summary["reconciled_count"])
        mc3.metric(
            "Discrepancies Found",
            summary["discrepancy_count"],
            delta=str(summary["discrepancy_count"]) if summary["discrepancy_count"] else None,
            delta_color="inverse",
        )

        if summary.get("results"):
            results_df = pd.DataFrame([
                {
                    "Product ID": r["product_id"],
                    "Internal Closing (g)": r["internal_closing"],
                    "Greenline Closing (g)": r["greenline_closing"],
                    "Delta (g)": r["delta"],
                    "Within Tolerance": "Yes" if r["is_within_tolerance"] else "No",
                }
                for r in summary["results"]
            ])
            st.dataframe(
                results_df,
                use_container_width=True,
                hide_index=True,
                column_config={
                    "Within Tolerance": st.column_config.TextColumn(width="small"),
                },
            )
