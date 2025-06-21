# pharmacist_roster_app.py (Complete Working Version)
import streamlit as st
import pandas as pd
import random
import sqlite3
from sqlite3 import Error
from datetime import datetime
import calendar
import xlsxwriter
import os
from dateutil.relativedelta import relativedelta
from io import BytesIO
from contextlib import contextmanager

# Initialize session state
if "edit_mode" not in st.session_state:
    st.session_state["edit_mode"] = False
if "pharmacists_df" not in st.session_state:
    st.session_state.pharmacists_df = pd.DataFrame(
        columns=["name", "last_unit", "last_night_call"]
    )

# Database Setup
@contextmanager
def db_session():
    """Context manager for database connections"""
    conn = None
    try:
        DB_PATH = os.path.abspath("pharmacist_roster.db")
        conn = sqlite3.connect(DB_PATH, check_same_thread=False)
        conn.execute("PRAGMA journal_mode=WAL")
        yield conn
    except Error as e:
        st.error(f"Database error: {e}")
        if conn:
            conn.rollback()
        st.stop()
    finally:
        if conn:
            conn.close()

def init_db():
    """Initialize database with correct schema"""
    with db_session() as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS pharmacists (
                name TEXT PRIMARY KEY,
                last_unit TEXT,
                last_night_call TEXT DEFAULT 'No',
                total_night_calls INTEGER DEFAULT 0
            )
        """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS roster_log (
                month TEXT PRIMARY KEY,
                roster_data BLOB,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """
        )
        conn.commit()

init_db()


def load_pharmacists():
    """Load pharmacists from database into session state"""
    with db_session() as conn:
        try:
            st.session_state.pharmacists_df = pd.read_sql(
                "SELECT name, last_unit, last_night_call FROM pharmacists ORDER BY name",
                conn,
            )
        except:
            st.session_state.pharmacists_df = pd.DataFrame(
                columns=["name", "last_unit", "last_night_call"]
            )


# Load initial data
if st.session_state.pharmacists_df.empty:
    load_pharmacists()

# Core Functions
def get_available(pharmacists, last_units, unit, exclude_list):
    """Get available pharmacists for assignment"""
    return [
        p
        for p in pharmacists
        if last_units.get(p, "") != unit and p not in exclude_list
    ]

def get_month_days(year, month):
    """Get all days in a month as datetime objects"""
    _, num_days = calendar.monthrange(year, month)
    return [datetime(year, month, day) for day in range(1, num_days + 1)]


def generate_roster(
    pharmacists, last_units, force_update=False, target_year=None, target_month=None
):
    """Generate monthly roster with proper transaction handling"""
    current_year = target_year or datetime.now().year
    current_month = target_month or datetime.now().month
    month_key = f"{current_year}-{current_month:02d}"

    with db_session() as conn:
        cursor = conn.cursor()

        # Check for existing roster
        if not force_update:
            cursor.execute(
                "SELECT roster_data FROM roster_log WHERE month = ?", (month_key,)
            )
            if result := cursor.fetchone():
                loaded_data = pd.read_pickle(BytesIO(result[0]))
                return (
                    loaded_data["calendar_df"],
                    loaded_data["night_calls"],
                    loaded_data["group_data"],
                )

        # Get previous month's assignments
        prev_date = datetime(current_year, current_month, 1) - relativedelta(months=1)
        prev_key = f"{prev_date.year}-{prev_date.month:02d}"

        actual_units = {}
        actual_night_status = {}

        cursor.execute(
            "SELECT roster_data FROM roster_log WHERE month = ?", (prev_key,)
        )
        if prev_result := cursor.fetchone():
            prev_data = pd.read_pickle(BytesIO(prev_result[0]))
            for row in prev_data["calendar_df"].itertuples(index=False):
                last_day = getattr(row, prev_data["calendar_df"].columns[-1])
                pharmacist_name = getattr(row, "Pharmacist", None) or getattr(
                    row, "index", None
                )

                if pd.notna(last_day) and pharmacist_name:
                    if "(N)" in str(last_day):
                        actual_night_status[pharmacist_name] = "Yes"
                        last_day = str(last_day).replace(" (N)", "")
                    else:
                        actual_night_status[pharmacist_name] = "No"

                    for unit in ["Dis1", "Dis2", "Dis3", "Store", "External"]:
                        if unit in str(last_day):
                            actual_units[pharmacist_name] = unit
                            break

        # Merge with current data
        effective_units = {**last_units, **actual_units}
        effective_night_status = {
            p: actual_night_status.get(p, "No") for p in pharmacists
        }

        # Assign external postings
        total_pharmacists = len(pharmacists)
        external_posting = random.sample(
            get_available(pharmacists, effective_units, "External", []),
            min(1, total_pharmacists),
        )
        remaining = [p for p in pharmacists if p not in external_posting]

        # Assign store
        store = random.sample(
            get_available(remaining, effective_units, "Store", []),
            min(1, len(remaining)),
        )
        remaining = [p for p in remaining if p not in store]

        # Assign dispensaries
        dispensaries = ["Dis1", "Dis2", "Dis3"]
        base_count = len(remaining) // 3
        remainder = len(remaining) % 3

        shift_groups = {}
        start = 0
        for i, dis in enumerate(dispensaries):
            count = base_count + (1 if i < remainder else 0)
            assigned = remaining[start : start + count]
            start += count

            half = (len(assigned) + 1) // 2
            shift_groups[dis] = {"AM": assigned[:half], "PM": assigned[half:]}

        # Build schedule
        full_schedule = {}
        days = get_month_days(current_year, current_month)

        for p in pharmacists:
            full_schedule[p] = {}
            for day in days:
                date_str = day.strftime("%Y-%m-%d")
                if p in external_posting:
                    full_schedule[p][date_str] = "External"
                elif p in store:
                    full_schedule[p][date_str] = "Store"
                else:
                    for dis, shifts in shift_groups.items():
                        if p in shifts["AM"] + shifts["PM"]:
                            week = (day.day - 1) // 7
                            is_am = (p in shifts["AM"]) != (week >= 2)
                            full_schedule[p][
                                date_str
                            ] = f"{dis} ({'AM' if is_am else 'PM'})"
                            break

        # Night call assignment
        eligible_for_calls = [p for p in pharmacists if p not in external_posting]
        nights = get_month_days(current_year, current_month)

        cursor.execute("SELECT name, total_night_calls FROM pharmacists")
        night_counts = {row[0]: row[1] for row in cursor.fetchall()}

        priority = sorted(
            eligible_for_calls,
            key=lambda x: (night_counts.get(x, 0), effective_night_status.get(x, "No")),
        )

        assigned_night_calls = {
            date.strftime("%Y-%m-%d"): priority[i % len(priority)]
            for i, date in enumerate(nights)
        }

        # Update night call counts
        update_data = []
        for p in eligible_for_calls:
            new_status = "Yes" if p in assigned_night_calls.values() else "No"
            update_data.append((new_status, 1 if new_status == "Yes" else 0, p))

        try:
            cursor.executemany(
                """
                UPDATE pharmacists 
                SET last_night_call = ?,
                    total_night_calls = total_night_calls + ?
                WHERE name = ?
            """,
                update_data,
            )
        except Error as e:
            st.error(f"🚨 Failed to update night call status: {str(e)}")
            conn.rollback()
            return None, None, None

        # Mark night shifts
        for date, p in assigned_night_calls.items():
            full_schedule[p][date] += " (N)"

        # Prepare roster data
        calendar_df = pd.DataFrame(full_schedule).T.reset_index()
        roster_data = {
            "calendar_df": calendar_df,
            "night_calls": assigned_night_calls,
            "group_data": shift_groups,
        }

        # Save to database
        try:
            pickle_buf = BytesIO()
            pd.to_pickle(roster_data, pickle_buf)

            cursor.execute(
                """
                INSERT OR REPLACE INTO roster_log (month, roster_data)
                VALUES (?, ?)
            """,
                (month_key, pickle_buf.getvalue()),
            )

            conn.commit()
            return calendar_df, assigned_night_calls, shift_groups

        except Error as e:
            conn.rollback()
            st.error(f"🚨 Failed to save roster: {str(e)}")
            return None, None, None


# UI Components
def show_pharmacist_tab():
    """Pharmacist management tab with persistent table"""
    st.subheader("👥 Pharmacist Management")

    # Clear Data Button
    if st.button("🧹 Clear All Data"):
        with st.expander("⚠️ Confirm Permanent Deletion", expanded=True):
            if st.checkbox("I understand this will delete ALL data irreversibly"):
                if st.button("✅ CONFIRM DELETION", type="primary"):
                    with db_session() as conn:
                        conn.execute("DELETE FROM pharmacists")
                        conn.execute("DELETE FROM roster_log")
                    load_pharmacists()  # Refresh the data
                    st.success("All data cleared successfully!")
                    st.balloons()

    # Add Pharmacist Form
    with st.form("add_pharmacist", clear_on_submit=True):
        cols = st.columns(3)
        with cols[0]:
            new_name = st.text_input(
                "Pharmacist Name", placeholder="Dr. Jane Doe", key="new_name"
            )
        with cols[1]:
            new_last_unit = st.selectbox(
                "Last Unit",
                ["", "Dis1", "Dis2", "Dis3", "Store", "External"],
                key="new_last_unit",
            )
        with cols[2]:
            new_last_night_call = st.selectbox(
                "Last Night Call", ["No", "Yes"], key="new_last_night_call"
            )

        if st.form_submit_button("➕ Add Pharmacist"):
            if new_name and new_name.strip():
                with db_session() as conn:
                    try:
                        cursor = conn.cursor()
                        cursor.execute(
                            "INSERT OR REPLACE INTO pharmacists (name, last_unit, last_night_call) VALUES (?, ?, ?)",
                            (new_name.strip(), new_last_unit, new_last_night_call),
                        )
                        conn.commit()
                        st.success(f"Added {new_name}")
                        load_pharmacists()  # Refresh the data
                    except Exception as e:
                        st.error(f"Failed to add pharmacist: {str(e)}")
            else:
                st.error("Name cannot be empty!")

    # Pharmacist Table Editor
    if not st.session_state.pharmacists_df.empty:
        edited_df = st.data_editor(
            st.session_state.pharmacists_df,
            num_rows="dynamic",
            use_container_width=True,
            key="pharmacist_editor",
        )

        if st.button("💾 Save Changes", key="save_pharmacists"):
            required_columns = ["name", "last_unit", "last_night_call"]
            if not all(col in edited_df.columns for col in required_columns):
                st.error("Error: Missing required columns!")
                return

            try:
                data = edited_df[required_columns].values.tolist()
                with db_session() as conn:
                    conn.execute("DELETE FROM pharmacists")
                    conn.executemany(
                        "INSERT INTO pharmacists (name, last_unit, last_night_call) VALUES (?, ?, ?)",
                        data,
                    )
                load_pharmacists()  # Refresh the data
                st.success("Pharmacist data updated!")
            except Exception as e:
                st.error(f"🚨 Failed to save changes: {str(e)}")
    else:
        st.info("No pharmacists added yet")

    # Always display the current table
    st.dataframe(st.session_state.pharmacists_df)


def show_roster_tab():
    """Roster generation tab"""
    st.subheader("🗓️ Roster Generation")

    # Date Selection
    current_year = datetime.now().year
    current_month = datetime.now().month
    years = list(range(current_year - 1, current_year + 2))
    months = list(range(1, 13))

    col_year, col_month = st.columns(2)
    with col_year:
        selected_year = st.selectbox("Year", years, index=years.index(current_year))
    with col_month:
        selected_month = st.selectbox("Month", months, index=current_month - 1)

    # Get current pharmacists
    if st.session_state.pharmacists_df.empty:
        st.warning("Please add pharmacists first")
        return

    col1, col2 = st.columns(2)
    with col1:
        if st.button("🔄 Generate Roster", type="primary"):
            with st.spinner("Generating roster..."):
                roster_df, night_calls, shift_groups = generate_roster(
                    st.session_state.pharmacists_df["name"].tolist(),
                    dict(
                        zip(
                            st.session_state.pharmacists_df["name"],
                            st.session_state.pharmacists_df["last_unit"],
                        )
                    ),
                    target_year=selected_year,
                    target_month=selected_month,
                )
            st.rerun()

    with col2:
        if st.button("🔁 Force New Roster", type="secondary"):
            with st.spinner("Generating new roster..."):
                roster_df, night_calls, shift_groups = generate_roster(
                    st.session_state.pharmacists_df["name"].tolist(),
                    dict(
                        zip(
                            st.session_state.pharmacists_df["name"],
                            st.session_state.pharmacists_df["last_unit"],
                        )
                    ),
                    force_update=True,
                    target_year=selected_year,
                    target_month=selected_month,
                )
            st.rerun()

    # Display Roster
    month_key = f"{selected_year}-{selected_month:02d}"
    with db_session() as conn:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT roster_data FROM roster_log WHERE month = ?", (month_key,)
        )
        if result := cursor.fetchone():
            loaded_data = pd.read_pickle(BytesIO(result[0]))
            roster_df = loaded_data["calendar_df"]

            st.dataframe(
                roster_df.style.applymap(
                    lambda x: "background-color: #FFCCCB" if "(N)" in str(x) else "",
                    subset=roster_df.columns[1:],
                ),
                height=600,
                use_container_width=True,
            )

            # Export Options
            st.subheader("💾 Export Options")
            export_roster(roster_df, loaded_data["group_data"])
        else:
            st.info("No roster generated for selected month")


def export_roster(roster_df, group_data):
    """Handle roster exports"""
    try:
        with BytesIO() as excel_buffer, BytesIO() as csv_buffer:
            # Excel Export
            with pd.ExcelWriter(excel_buffer, engine="xlsxwriter") as writer:
                roster_df.to_excel(writer, index=False, sheet_name="Roster")

                workbook = writer.book
                red_fmt = workbook.add_format({"bg_color": "#FFCCCB"})
                worksheet = writer.sheets["Roster"]

                for row_idx in range(1, len(roster_df) + 1):
                    for col_idx, day in enumerate(roster_df.columns[1:], 1):
                        cell_val = roster_df.iloc[row_idx - 1, col_idx]
                        if "(N)" in str(cell_val):
                            worksheet.write(row_idx, col_idx, cell_val, red_fmt)

                pd.DataFrame(
                    [
                        {"Unit": k, "Shift": sk, "Pharmacists": ", ".join(sv)}
                        for k, v in group_data.items()
                        for sk, sv in v.items()
                    ]
                ).to_excel(writer, index=False, sheet_name="Shift Groups")

            # CSV Export
            roster_df.to_csv(csv_buffer, index=False)

            # Download Buttons
            col1, col2 = st.columns(2)
            with col1:
                st.download_button(
                    "📊 Download Excel",
                    excel_buffer.getvalue(),
                    f"roster_{datetime.now().strftime('%Y-%m')}.xlsx",
                    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                )
            with col2:
                st.download_button(
                    "📝 Download CSV",
                    csv_buffer.getvalue(),
                    f"roster_{datetime.now().strftime('%Y-%m')}.csv",
                    "text/csv",
                )
    except Exception as e:
        st.error(f"🚨 Export failed: {str(e)}")


def show_analytics_tab():
    """Analytics and reporting tab"""
    st.subheader("📊 Analytics")

    # Get current roster data
    current_year = datetime.now().year
    current_month = datetime.now().month
    month_key = f"{current_year}-{current_month:02d}"

    with db_session() as conn:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT roster_data FROM roster_log WHERE month = ?", (month_key,)
        )
        if result := cursor.fetchone():
            loaded_data = pd.read_pickle(BytesIO(result[0]))
            roster_df = loaded_data["calendar_df"]

            tab1, tab2 = st.tabs(["Night Call Frequency", "Assignment Distribution"])

            with tab1:
                st.markdown("#### Night Call Frequency")
                night_calls = (
                    roster_df.iloc[:, 1:]
                    .applymap(lambda x: "(N)" in str(x))
                    .sum(axis=1)
                )
                night_call_df = pd.DataFrame(
                    {"Pharmacist": roster_df.iloc[:, 0], "Night Calls": night_calls}
                )
                st.dataframe(night_call_df)
                st.bar_chart(night_call_df.set_index("Pharmacist"))

            with tab2:
                st.markdown("#### Assignment Distribution")
                assignment_counts = (
                    roster_df.iloc[:, 1:]
                    .apply(pd.Series.value_counts)
                    .fillna(0)
                    .sum(axis=1)
                )
                st.bar_chart(assignment_counts)
        else:
            st.info("No roster data available for analysis")


def show_settings_tab():
    """Application settings tab"""
    st.subheader("⚙️ Settings")

    with st.expander("Hospital Configuration"):
        st.number_input(
            "Number of Dispensaries", min_value=1, value=3, key="num_dispensaries"
        )
        st.text_input(
            "Dispensary Names", value="Dis1, Dis2, Dis3", key="dispensary_names"
        )
        st.number_input("Number of Stores", min_value=1, value=3, key="num_stores")
        st.text_input("Store Names", value="Str1, Str2, Str3", key="store_names")

    with st.expander("Roster Rules"):
        st.number_input(
            "Maximum Night Calls", min_value=1, value=5, key="max_night_calls"
        )
        st.checkbox(
            "Prevent Consecutive Night Calls",
            value=True,
            key="prevent_consecutive_nights",
        )


# Main App
def main():
    st.set_page_config(
        page_title="Pharmacist Roster App", layout="wide", page_icon="📅"
    )
    st.title("📅 Pharmacist Roster Manager")

    tab1, tab2, tab3, tab4 = st.tabs(
        ["👥 Pharmacists", "🗓️ Roster", "📊 Analytics", "⚙️ Settings"]
    )

    with tab1:
        show_pharmacist_tab()

    with tab2:
        show_roster_tab()

    with tab3:
        show_analytics_tab()

    with tab4:
        show_settings_tab()


if __name__ == "__main__":
    main()
