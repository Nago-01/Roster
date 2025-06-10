# pharmacist_roster_app.py
import streamlit as st
import pandas as pd
import random
import sqlite3
from sqlite3 import Error
from datetime import datetime, timedelta
import calendar
import xlsxwriter
from dateutil.relativedelta import relativedelta

# Attempt to import StringIO and BytesIO for compatibility
try:
    from io import StringIO, BytesIO
except ImportError:
    # Fallback for environments with broken io module
    import io

    StringIO = io.StringIO
    BytesIO = io.BytesIO


from contextlib import contextmanager


@contextmanager
def db_session():
    """Context manager for database sessions"""
    try:
        verify_connection()
        yield conn
    except Error as e:
        conn.rollback()
        handle_db_error(f"Database operation failed: {e}")
    finally:
        pass  # Let explicit close handle this


# DATABASE SETUP
def create_connection():
    """Create or refresh a database connection with better error handling"""
    global conn  # Ensure we're modifying the global connection

    try:
        # Close existing connection if it exists
        if "conn" in globals() and conn:
            try:
                conn.close()
            except Error:
                pass

        # Create new connection with optimized settings
        conn = sqlite3.connect(
            "pharmacist_roster.db",
            check_same_thread=False,
            timeout=30,
            isolation_level=None,
        )  # For better transaction control
        conn.execute("PRAGMA journal_mode=WAL")  # Better concurrency
        return conn
    except Error as e:
        handle_db_error(f"Database connection failed: {e}")
        return None


def verify_connection():
    """Check if connection is alive"""
    global conn

    try:
        conn.execute("SELECT 1")
    except:

        conn = create_connection()
        if not conn:
            handle_db_error("Database connection failed")


def init_db():
    """Initialize database tables with schema validation"""
    conn = create_connection()
    if not conn:
        st.stop()

    try:
        cursor = conn.cursor()

        cursor.execute(
            """
        CREATE TABLE IF NOT EXISTS pharmacists (
            name TEXT PRIMARY KEY,
            last_unit TEXT,
            last_night_call TEXT DEFAULT 'No'
        )"""
        )

        cursor.execute(
            """
        CREATE TABLE IF NOT EXISTS roster_log (
            month TEXT PRIMARY KEY,
            roster_data BLOB
        )"""
        )

        # Verify columns exist
        cursor.execute("PRAGMA table_info(pharmacists)")
        columns = {col[1] for col in cursor.fetchall()}
        if "last_night_call" not in columns:
            cursor.execute(
                """
            ALTER TABLE pharmacists 
            ADD COLUMN last_night_call TEXT DEFAULT 'No'
            """
            )

        conn.commit()
        return conn
    except Error as e:
        st.error(f"🚨 Database initialization failed: {e}")
        conn.rollback()
        st.stop()


conn = init_db()


def handle_db_error(error_msg):
    """Standardized database error handler"""
    st.error(f"🚨 {error_msg}")
    if conn:  # Safely close connection if exists
        try:
            conn.close()
        except:
            pass
    st.stop()


# CORE LOGIC FUNCTIONS


def get_available(pharmacists, last_units, unit, exclude_list):
    return [
        p
        for p in pharmacists
        if last_units.get(p, "") != unit and p not in exclude_list
    ]


def get_month_days(year, month):
    _, num_days = calendar.monthrange(year, month)
    return [datetime(year, month, day) for day in range(1, num_days + 1)]


def generate_roster(pharmacists, last_units, force_update=False):
    verify_connection()
    current_year = datetime.now().year
    current_month = datetime.now().month
    month_key = f"{current_year}-{current_month:02d}"
    cursor = conn.cursor()

    # CHECK FOR EXISTING ROSTER
    if not force_update:
        try:
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
        except Error as e:
            st.warning(f"⚠️ Roster load failed: {e}")

    # GET PREVIOUS MONTH'S ACTUAL ASSIGNMENTS
    prev_date = datetime.now() - relativedelta(months=1)
    prev_month = prev_date.month
    prev_year = prev_date.year
    prev_key = f"{prev_year}-{prev_month:02d}"

    actual_units = {}
    actual_night_status = {}

    try:
        cursor.execute(
            "SELECT roster_data FROM roster_log WHERE month = ?", (prev_key,)
        )
        if prev_result := cursor.fetchone():
            prev_data = pd.read_pickle(BytesIO(prev_result[0]))
            for _, row in prev_data["calendar_df"].iterrows():
                last_day = row.iloc[-1]
                if pd.notna(last_day):
                    # Capture night call status
                    if "(N)" in str(last_day):
                        actual_night_status[row["Pharmacist"]] = "Yes"
                        last_day = last_day.replace(" (N)", "")
                    else:
                        actual_night_status[row["Pharmacist"]] = "No"

                    # Capture unit assignments
                    if "Dis1" in last_day:
                        actual_units[row["Pharmacist"]] = "Dis1"
                    elif "Dis2" in last_day:
                        actual_units[row["Pharmacist"]] = "Dis2"
                    elif "Dis3" in last_day:
                        actual_units[row["Pharmacist"]] = "Dis3"
                    elif "Store" in last_day:
                        actual_units[row["Pharmacist"]] = "Store"
                    elif "External" in last_day:
                        actual_units[row["Pharmacist"]] = "External"
    except Error as e:
        st.warning(f"⚠️ Previous month data load failed: {e}")

    # USE ACTUAL DATA WHERE AVAILABLE
    effective_units = {**last_units, **actual_units}
    effective_night_status = {p: actual_night_status.get(p, "No") for p in pharmacists}

    # PHARMACIST ASSIGNMENT (YOUR ORIGINAL LOGIC)
    total_pharmacists = len(pharmacists)
    external_posting = random.sample(
        get_available(pharmacists, effective_units, "External", []),
        min(10, total_pharmacists),
    )
    remaining = [p for p in pharmacists if p not in external_posting]

    store = random.sample(
        get_available(remaining, effective_units, "Store", []), min(3, len(remaining))
    )
    remaining = [p for p in remaining if p not in store]

    # DISPENSARY ASSIGNMENT (YOUR ORIGINAL LOGIC)
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

    # BUILD SCHEDULE (YOUR ORIGINAL LOGIC)
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

    # NIGHT CALL ASSIGNMENT (MODIFIED TO USE EFFECTIVE STATUS)
    eligible_for_calls = [p for p in pharmacists if p not in external_posting]
    nights = get_month_days(current_year, current_month)

    priority = [
        p for p in eligible_for_calls if effective_night_status.get(p, "No") != "Yes"
    ]

    pharmacist_cycle = priority + [p for p in eligible_for_calls if p not in priority]
    assigned_night_calls = {
        date.strftime("%Y-%m-%d"): pharmacist_cycle[i % len(pharmacist_cycle)]
        for i, date in enumerate(nights)
    }

    # UPDATE STATUS (ONLY ELIGIBLE PHARMACISTS)
    try:
        for p in eligible_for_calls:
            cursor.execute(
                "UPDATE pharmacists SET last_night_call=? WHERE name=?",
                ("Yes" if p in assigned_night_calls.values() else "No", p),
            )
        conn.commit()
    except Error as e:
        st.error(f"🚨 Failed to update night call status: {e}")
        conn.rollback()
        return None, None, None

    # Mark night shifts
    for date, p in assigned_night_calls.items():
        full_schedule[p][date] += " (N)"

    # SAVE ROSTER
    calendar_df = pd.DataFrame(full_schedule).T.reset_index()
    roster_data = {
        "calendar_df": calendar_df,
        "night_calls": assigned_night_calls,
        "group_data": shift_groups,
    }

    try:
        buf = BytesIO()
        pd.to_pickle(roster_data, buf)
        cursor.execute(
            "REPLACE INTO roster_log (month, roster_data) VALUES (?, ?)",
            (month_key, buf.getvalue()),
        )
        conn.commit()
    except Error as e:
        st.error(f"🚨 Failed to save roster: {e}")
        conn.rollback()
        return None, None, None

    return calendar_df, assigned_night_calls, shift_groups


# STREAMLIT UI

st.set_page_config(page_title="Pharmacist Roster App", layout="wide")
st.title("📅 Pharmacist Roster Generator")

# 1. DATA MANAGEMENT
if st.button("🧹 Clear All Data"):
    with st.expander("⚠️ Confirm Permanent Deletion", expanded=True):
        if st.checkbox("I understand this will delete ALL data irreversibly"):
            if st.button("✅ CONFIRM DELETION", type="primary"):
                cursor = conn.cursor()
                cursor.execute("DELETE FROM pharmacists")
                cursor.execute("DELETE FROM roster_log")
                conn.commit()
                st.success("All data cleared successfully!")
                st.balloons()

# PHARMACIST MANAGEMENT
with st.form("add_pharmacist", clear_on_submit=True):
    cols = st.columns(3)
    with cols[0]:
        new_name = st.text_input("Pharmacist Name", placeholder="Dr. Jane Doe")
    with cols[1]:
        new_last_unit = st.selectbox(
            "Last Unit", ["", "Dis1", "Dis2", "Dis3", "Store", "External"]
        )
    with cols[2]:
        new_last_night_call = st.selectbox("Last Night Call", ["No", "Yes"])

    if st.form_submit_button("➕ Add Pharmacist") and new_name:
        cursor = conn.cursor()
        cursor.execute(
            "INSERT OR REPLACE INTO pharmacists VALUES (?, ?, ?)",
            (new_name.strip(), new_last_unit, new_last_night_call),
        )
        conn.commit()
        st.success(f"Added {new_name}")

# PHARMACIST EDITING
pharmacists_df = pd.read_sql("SELECT * FROM pharmacists ORDER BY name", conn)
if not pharmacists_df.empty:
    edited_df = st.data_editor(
        pharmacists_df, num_rows="dynamic", use_container_width=True
    )
    if st.button("💾 Save Changes"):
        verify_connection()
        try:
            # Refresh connection to prevent stale connections
            cursor = conn.cursor()

            # 1. Verify required columns exist
            required_columns = ["name", "last_unit", "last_night_call"]
            if not all(col in edited_df.columns for col in required_columns):
                st.error("Error: Missing required columns in the data!")
                st.stop()

            # 2. Convert DataFrame to list of tuples
            data = edited_df[required_columns].values.tolist()

            # 3. Validate data types
            for i, row in enumerate(data):
                if not isinstance(row[0], str) or len(row[0].strip()) == 0:
                    st.error(f"Row {i+1}: Invalid pharmacist name '{row[0]}'")
                    st.stop()

            # 4. Update database
            cursor.execute("DELETE FROM pharmacists")
            cursor.executemany("INSERT INTO pharmacists VALUES (?, ?, ?)", data)
            conn.commit()
            st.success("Pharmacist data updated successfully!")
            st.rerun()  # Refresh the UI

        except Exception as e:
            st.error(f"🚨 Failed to save changes: {str(e)}")
            conn.rollback()
            # Debugging info
            st.error(
                f"Problematic data sample: {data[:3] if 'data' in locals() else 'N/A'}"
            )

# ROSTER GENERATION
col1, col2 = st.columns(2)
with col1:
    if st.button("🔄 Generate Monthly Roster", type="primary"):
        if pharmacists_df.empty:
            st.error("No pharmacists available!")
        else:
            roster_df, night_calls, shift_groups = generate_roster(
                pharmacists_df["name"].tolist(),
                dict(zip(pharmacists_df["name"], pharmacists_df["last_unit"])),
                force_update=False,
            )
            st.rerun()

with col2:
    if st.button(
        "🔁 Force New Roster",
        type="secondary",
        help="Generate a completely new roster for this month",
    ):
        if pharmacists_df.empty:
            st.error("No pharmacists available!")
        else:
            roster_df, night_calls, shift_groups = generate_roster(
                pharmacists_df["name"].tolist(),
                dict(zip(pharmacists_df["name"], pharmacists_df["last_unit"])),
                force_update=True,
            )
            st.rerun()

# Display current roster if exists
try:
    current_year = datetime.now().year
    current_month = datetime.now().month
    month_key = f"{current_year}-{current_month:02d}"
    cursor = conn.cursor()
    cursor.execute("SELECT roster_data FROM roster_log WHERE month = ?", (month_key,))
    if result := cursor.fetchone():
        loaded_data = pd.read_pickle(BytesIO(result[0]))
        roster_df = loaded_data["calendar_df"]

        st.subheader("📅 Current Monthly Roster")
        st.dataframe(
            roster_df.style.applymap(
                lambda x: "background-color: #FFCCCB" if "(N)" in str(x) else "",
                subset=roster_df.columns[1:],
            ),
            use_container_width=True,
            height=600,
        )

        # Export options
        st.subheader("💾 Export Options")

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
                            for k, v in loaded_data["group_data"].items()
                            for sk, sv in v.items()
                        ]
                    ).to_excel(writer, index=False, sheet_name="Shift Groups")

                # CSV Export
                roster_df.to_csv(csv_buffer, index=False)

                # Create download buttons
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

                st.success("Exports generated successfully!")

        except Exception as e:
            st.error(f"🚨 Export failed: {str(e)}")
            st.stop()  # Halt execution if exports fail

except Error as e:
    st.warning("No roster generated for this month yet")

# Final connection cleanup
try:
    if conn:
        conn.commit()
except Error as e:
    st.error(f"🚨 Final commit failed: {e}")
finally:
    conn.close() if conn else None
