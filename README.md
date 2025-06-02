# **Automated Pharmacist Roster System**  
*A Streamlit-powered tool for fair shift scheduling in hospital pharmacies*

## **Key Features**  
✅ **Automated Monthly Scheduling** - Generates balanced AM/PM dispensary rotations  
✅ **Night Shift Management** - Tracks and rotates night call assignments fairly  
✅ **Unit-Based Assignments** - Distributes pharmacists across:  
   - 3 Dispensaries (Dis1/Dis2/Dis3)  
   - Store  
   - External postings  
✅ **Data Persistence** - SQLite database tracks:  
   - Pharmacist profiles  
   - Last assigned units  
   - Night call history  
✅ **Multi-Format Export** - Download as:  
   - Excel (with night shift highlighting)  
   - CSV  
   - ICS calendar file  

## **How It Works**  
1. **Input Pharmacist List** via UI (name + last unit assignment)  
2. **Algorithm Logic**:  
   - Avoids consecutive same-unit assignments  
   - Rotates night calls equitably (prioritizes those who haven't done recent nights)  
   - Splits dispensary staff into AM/PM groups alternating biweekly  
3. **Output**: Interactive calendar view + exportable files  

## **Quick Start**  
1. Install requirements:  
   ```bash
   pip install streamlit pandas sqlite3 ics xlsxwriter
   ```
2. Run the app:  
   ```bash
   streamlit run pharmacist_roster_app.py
   ```
3. Access UI at `localhost:8501`



## **📂 Database Schema**  
- `pharmacists` table:  
  ```sql
  (name TEXT PRIMARY KEY, last_unit TEXT, last_night_call TEXT)
  ```  
- `roster_log` table:  
  ```sql
  (month TEXT PRIMARY KEY, roster_data BLOB)
  ```  

## **⚠️ Current Limitations**  
- No leave request handling  
- Manual pharmacist entry required  
- Fixed to current month only  
