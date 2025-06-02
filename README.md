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

## **Installation**  
```bash
pip install streamlit pandas sqlite3 ics xlsxwriter
```

## **Quick Start**  
1. Install requirements (see above)
2. Run the app:  
   ```bash
   streamlit run pharmacist_roster_app.py
   ```
3. Access UI at `localhost:8501`

## **Troubleshooting**  
If you get `ModuleNotFoundError: No module named 'ics'`:  
```bash
pip install ics  # Install the missing package
```

## **Database Schema**  
- `pharmacists` table:  
  ```sql
  (name TEXT PRIMARY KEY, last_unit TEXT, last_night_call TEXT)
  ```  
- `roster_log` table:  
  ```sql
  (month TEXT PRIMARY KEY, roster_data BLOB)
  ```  

## **📜 License**  
MIT License - Free for hospital/academic use  

---
