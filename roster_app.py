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
from datetime import date

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


def generate_roster(
    pharmacists,
    last_units,
    force_update=False,
    target_year=None,
    target_month=None,
    dispensary_list=None,
    store_list=None,
    num_external=1,
):
    verify_connection()
    if target_year is None or target_month is None:
        current_year = datetime.now().year
        current_month = datetime.now().month
    else:
        current_year = target_year
        current_month = target_month
    month_key = f"{current_year}-{current_month:02d}"
    cursor = conn.cursor()

    # CHECK FOR EXISTING ROSTER
    if not force_update:
        cursor.execute(
            "SELECT roster_data FROM roster_log WHERE month = ?", (month_key,)
        )
        if result := cursor.fetchone():
            loaded_data = pd.read_pickle(BytesIO(result[0]))

            # ALSO update pharmacists table based on loaded_data
            full_schedule = loaded_data["calendar_df"].set_index("index").T.to_dict()

            try:
                for name, days in full_schedule.items():
                    # Most frequent assignment (cleaned)
                    assignments = list(days.values())
                    primary_unit = max(set(assignments), key=assignments.count)
                    clean_unit = (
                        primary_unit.split("(")[0].strip()
                        if "(" in primary_unit
                        else primary_unit
                    )
                    night_call_status = (
                        "Yes" if any("(N)" in val for val in assignments) else "No"
                    )
                    cursor.execute(
                        """UPDATE pharmacists 
                        SET last_unit = ?, 
                            last_night_call = ? 
                        WHERE name = ?""",
                        (clean_unit, night_call_status, name),
                    )
                conn.commit()
            except Error as e:
                st.warning(f"⚠️ Failed to sync pharmacist table from saved roster: {e}")
                conn.rollback()

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
            for row in prev_data["calendar_df"].itertuples(index=False):
                last_day = getattr(row, prev_data["calendar_df"].columns[-1])
                pharmacist_name = getattr(row, "Pharmacist", None) or getattr(
                    row, "index", None
                )
                if pd.notna(last_day) and pharmacist_name:
                    # Capture night call status
                    if "(N)" in str(last_day):
                        actual_night_status[pharmacist_name] = "Yes"
                        last_day = last_day.replace(" (N)", "")
                    else:
                        actual_night_status[pharmacist_name] = "No"

                    # Capture unit assignments
                    if "Dis1" in last_day:
                        actual_units[pharmacist_name] = "Dis1"
                    elif "Dis2" in last_day:
                        actual_units[pharmacist_name] = "Dis2"
                    elif "Dis3" in last_day:
                        actual_units[pharmacist_name] = "Dis3"
                    elif "Store" in last_day:
                        actual_units[pharmacist_name] = "Store"
                    elif "External" in last_day:
                        actual_units[pharmacist_name] = "External"
    except Error as e:
        st.warning(f"⚠️ Previous month data load failed: {e}")

    # USE ACTUAL DATA WHERE AVAILABLE
    effective_units = {**last_units, **actual_units}
    effective_night_status = {p: actual_night_status.get(p, "No") for p in pharmacists}

    # PHARMACIST ASSIGNMENT (YOUR ORIGINAL LOGIC)
    total_pharmacists = len(pharmacists)
    external_posting = random.sample(
        get_available(pharmacists, effective_units, "External", []),
        min(num_external, total_pharmacists),
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
        # Update last_unit and last_night_call for all pharmacists in a transaction
        with conn:
            for pharmacist in pharmacists:
                # Get assigned unit from the new roster (most frequent assignment)
                assignments = list(full_schedule[pharmacist].values())
                primary_unit = max(set(assignments), key=assignments.count)

                # Clean unit string (remove shift info)
                clean_unit = (
                    primary_unit.split("(")[0].strip()
                    if "(" in primary_unit
                    else primary_unit
                )

                # Determine night call status
                night_call_status = (
                    "Yes" if any("(N)" in day for day in assignments) else "No"
                )

                # Only update if changed
                cursor.execute(
                    "SELECT last_unit, last_night_call FROM pharmacists WHERE name=?",
                    (pharmacist,),
                )
                row = cursor.fetchone()
                if not row or row[0] != clean_unit or row[1] != night_call_status:
                    cursor.execute(
                        """UPDATE pharmacists 
                        SET last_unit = ?, 
                            last_night_call = ? 
                        WHERE name = ?""",
                        (clean_unit, night_call_status, pharmacist),
                    )

            # ===== NEW VERIFICATION =====
            cursor.execute("SELECT COUNT(*) FROM pharmacists")
            pharmacist_count = cursor.fetchone()[0]
            if pharmacist_count != len(pharmacists):
                handle_db_error(
                    f"""Synchronization failed! 
                    Database: {pharmacist_count} pharmacists vs Roster: {len(pharmacists)}"""
                )
                return None, None, None
    except Error as e:
        conn.rollback()
        st.error(f"🚨 Failed to update pharmacist records: {e}")
        return None, None, None

    return calendar_df, assigned_night_calls, shift_groups


# STREAMLIT UI
st.set_page_config(page_title="Pharmacist Roster App", layout="wide")
st.title("📅 Pharmacist Roster Generator")

main_tab, roster_tab, analytics_tab, settings_tab = st.tabs(
    ["👥 Pharmacists", "🗓️ Roster", "📊 Analytics", "⚙️ Settings"]
)

with main_tab:
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

with roster_tab:
    # ROSTER GENERATION

    # Add selectors for year and month
    current_year = datetime.now().year
    current_month = datetime.now().month
    years = list(range(current_year - 2, current_year + 3))
    months = list(range(1, 13))

    col_year, col_month = st.columns(2)
    with col_year:
        selected_year = st.selectbox(
            "Select Year", years, index=years.index(current_year)
        )
    with col_month:
        selected_month = st.selectbox("Select Month", months, index=current_month - 1)

    col1, col2 = st.columns(2)

    def validate_constraints(
        pharmacists_df, num_dispensaries=3, num_store=1, num_external=1
    ):
        total_needed = num_dispensaries + num_store + num_external
        total_available = len(pharmacists_df)
        if total_available < total_needed:
            return (
                False,
                f"Not enough pharmacists! Needed: {total_needed}, Available: {total_available}",
            )
        return True, ""

    with col1:
        if st.button(
            "🔄 Generate Monthly Roster",
            type="primary",
            help="Generate the roster for the selected month and year",
        ):
            if pharmacists_df.empty:
                st.error("No pharmacists available!")
            else:
                # Example usage before generating roster
                num_dispensaries = st.session_state.get("num_dispensaries", 3)
                dispensary_list = st.session_state.get(
                    "dispensary_list", ["Dis1", "Dis2", "Dis3"]
                )
                num_store = st.session_state.get("num_store", 1)
                store_list = st.session_state.get("store_list", ["Store"])
                num_external = st.session_state.get("num_external", 1)

                valid, msg = validate_constraints(
                    pharmacists_df,
                    num_dispensaries=num_dispensaries,
                    num_store=num_store,
                    num_external=num_external,
                )
                if not valid:
                    st.error(msg)
                else:
                    roster_df, night_calls, shift_groups = generate_roster(
                        pharmacists_df["name"].tolist(),
                        dict(zip(pharmacists_df["name"], pharmacists_df["last_unit"])),
                        force_update=False,
                        target_year=selected_year,
                        target_month=selected_month,
                        dispensary_list=dispensary_list,
                        store_list=store_list,
                        num_external=num_external,
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
                    target_year=selected_year,
                    target_month=selected_month,
                )
                st.rerun()

    # Display current roster if exists
    try:
        current_year = datetime.now().year
        current_month = datetime.now().month
        month_key = f"{current_year}-{current_month:02d}"
        cursor = conn.cursor()
        cursor.execute(
            "SELECT roster_data FROM roster_log WHERE month = ?", (month_key,)
        )
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

    # Validation function
    def validate_roster(roster_df):
        errors = []
        # Check for duplicate assignments per day
        for day in roster_df.columns[1:-1]:  # Exclude index and Lock columns
            assignments = roster_df[day].tolist()
            seen = set()
            for a in assignments:
                if a in seen and a != "" and pd.notna(a):
                    errors.append(f"Duplicate assignment '{a}' on {day}")
                seen.add(a)
        # Check for consecutive night calls
        for idx, row in roster_df.iterrows():
            night_days = [i for i, val in enumerate(row[1:-1]) if "(N)" in str(val)]
            if any(b == a + 1 for a, b in zip(night_days, night_days[1:])):
                errors.append(f"{row[0]} has consecutive night calls.")
        # Check for unassigned shifts
        for day in roster_df.columns[1:-1]:
            if any(pd.isna(val) or val == "" for val in roster_df[day]):
                errors.append(f"Unassigned shift(s) on {day}")
        return errors

    # After roster_df is available (after generation or manual edit)
    if "roster_df" in locals():
        errors = validate_roster(roster_df)

        def check_workload_balance(roster_df, max_night_calls=5):
            night_calls = (
                roster_df.iloc[:, 1:-1].applymap(lambda x: "(N)" in str(x)).sum(axis=1)
            )
            overloaded = roster_df.iloc[:, 0][night_calls > max_night_calls].tolist()
            return overloaded

        overloaded = check_workload_balance(roster_df)
        if errors:
            for err in errors:
                st.warning(f"⚠️ {err}")
        if overloaded:
            st.warning(
                f"⚠️ Overloaded pharmacists (too many night calls): {', '.join(overloaded)}"
            )

    # After displaying the current roster...

    if "edit_mode" not in st.session_state:
        st.session_state["edit_mode"] = False

    if st.button("✏️ Edit Assignments"):
        st.session_state["edit_mode"] = True

    if st.session_state["edit_mode"]:
        st.subheader("Manual Assignment Override")
        # Add a 'Lock' column if not present
        if "Lock" not in roster_df.columns:
            roster_df["Lock"] = False

        edited_roster = st.data_editor(
            roster_df, num_rows="dynamic", use_container_width=True, key="edit_roster"
        )

        if st.button("💾 Save Manual Overrides"):
            # Conflict detection example
            conflict = False
            for day in roster_df.columns[1:-1]:  # Exclude index and Lock columns
                assignments = edited_roster[day].tolist()
                if len(assignments) != len(set(assignments)):
                    st.error(f"Conflict detected: Duplicate assignment on {day}")
                    conflict = True
                    break

            if not conflict:
                # Validate and clean data before saving
                try:
                    for idx, row in edited_roster.iterrows():
                        # Skip if locked
                        if row.get("Lock"):
                            continue

                        # Validate pharmacist name
                        if (
                            not isinstance(row["Pharmacist"], str)
                            or not row["Pharmacist"].strip()
                        ):
                            st.error(f"Invalid pharmacist name at row {idx + 1}")
                            raise ValueError("Invalid data")

                        # Validate unit assignments
                        for day in roster_df.columns[
                            1:-1
                        ]:  # Exclude index and Lock columns
                            if pd.isna(row[day]) or row[day] == "":
                                continue  # Skip empty assignments
                            if not isinstance(row[day], str):
                                st.error(
                                    f"Invalid assignment type at row {idx + 1}, day {day}"
                                )
                                raise ValueError("Invalid data")

                    # If validation passes, save the edited roster
                    try:
                        with conn:
                            # Clear existing data for the month
                            cursor.execute(
                                "DELETE FROM roster_log WHERE month = ?", (month_key,)
                            )

                            # Insert new roster data
                            cursor.execute(
                                "INSERT INTO roster_log (month, roster_data) VALUES (?, ?)",
                                (
                                    month_key,
                                    sqlite3.Binary(pd.util.pickle.dumps(edited_roster)),
                                ),
                            )
                        conn.commit()
                        st.success("Manual overrides saved successfully!")
                        st.session_state["edit_mode"] = False  # Exit edit mode
                        st.rerun()  # Refresh the UI
                    except Error as e:
                        st.error(f"🚨 Failed to save roster: {e}")
                        conn.rollback()
                except Exception as e:
                    st.error(f"🚨 Data validation error: {e}")


with analytics_tab:
    # Validation function
    def validate_roster(roster_df):
        errors = []
        # Check for duplicate assignments per day
        for day in roster_df.columns[1:-1]:  # Exclude index and Lock columns
            assignments = roster_df[day].tolist()
            seen = set()
            for a in assignments:
                if a in seen and a != "" and pd.notna(a):
                    errors.append(f"Duplicate assignment '{a}' on {day}")
                seen.add(a)
        # Check for consecutive night calls
        for idx, row in roster_df.iterrows():
            night_days = [i for i, val in enumerate(row[1:-1]) if "(N)" in str(val)]
            if any(b == a + 1 for a, b in zip(night_days, night_days[1:])):
                errors.append(f"{row[0]} has consecutive night calls.")
        # Check for unassigned shifts
        for day in roster_df.columns[1:-1]:
            if any(pd.isna(val) or val == "" for val in roster_df[day]):
                errors.append(f"Unassigned shift(s) on {day}")
        return errors

    def check_workload_balance(roster_df, max_night_calls=5):
        night_calls = (
            roster_df.iloc[:, 1:-1].applymap(lambda x: "(N)" in str(x)).sum(axis=1)
        )
        overloaded = roster_df.iloc[:, 0][night_calls > max_night_calls].tolist()
        return overloaded

    # Place this after your main UI sections

    st.subheader("📊 Analytics & Reporting")

    tab1, tab2, tab3 = st.tabs(
        ["Assignment Distribution", "Night Call Frequency", "Unit History"]
    )

    with tab1:
        st.markdown("#### Assignment Distribution")
        if "roster_df" in locals():
            assignment_counts = (
                roster_df.iloc[:, 1:-1]
                .apply(pd.Series.value_counts)
                .fillna(0)
                .sum(axis=1)
            )
            st.bar_chart(assignment_counts)
        else:
            st.info("No roster loaded for analysis.")

    with tab2:
        st.markdown("#### Night Call Frequency")
        if "roster_df" in locals():
            night_calls = (
                roster_df.iloc[:, 1:-1].applymap(lambda x: "(N)" in str(x)).sum(axis=1)
            )
            night_call_df = pd.DataFrame(
                {"Pharmacist": roster_df.iloc[:, 0], "Night Calls": night_calls}
            )
            st.dataframe(night_call_df)
            st.bar_chart(night_call_df.set_index("Pharmacist"))
        else:
            st.info("No roster loaded for analysis.")

    with tab3:
        st.markdown("#### Unit Assignment History")
        if "roster_df" in locals():
            # Show a table of each pharmacist's unit assignments over the month
            st.dataframe(roster_df)
        else:
            st.info("No roster loaded for analysis.")

with settings_tab:
    st.subheader("🏥 Hospital Configuration")

    num_dispensaries = st.number_input(
        "Number of Dispensaries", min_value=1, max_value=10, value=3, step=1
    )
    dispensary_names = st.text_input(
        "Dispensary Names (comma-separated)", value="Dis1,Dis2,Dis3"
    )
    dispensary_list = [d.strip() for d in dispensary_names.split(",") if d.strip()]

    num_store = st.number_input(
        "Number of Store(s)", min_value=1, max_value=5, value=1, step=1
    )
    store_names = st.text_input("Store Names (comma-separated)", value="Store")
    store_list = [s.strip() for s in store_names.split(",") if s.strip()]

    num_external = st.number_input(
        "Number of Pharmacists for External Posting",
        min_value=0,
        max_value=20,
        value=1,
        step=1,
    )

    st.info(
        "These settings will be used for the next roster generation. "
        "Make sure to save your changes before generating a new roster."
    )

    # Save settings to session state for use in other tabs
    st.session_state["num_dispensaries"] = num_dispensaries
    st.session_state["dispensary_list"] = dispensary_list
    st.session_state["num_store"] = num_store
    st.session_state["store_list"] = store_list
    st.session_state["num_external"] = num_external

# Final connection cleanup
try:
    if conn:
        conn.commit()
except Error as e:
    st.error(f"🚨 Final commit failed: {e}")
finally:
    conn.close() if conn else None
