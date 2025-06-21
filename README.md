# **Pharmacist Roster System**

A user-friendly Streamlit app for fair, flexible, and automated pharmacist scheduling in hospitals.

---

## **Features**

- **Automated Monthly Scheduling**  
  Generate balanced AM/PM dispensary, store, and external posting assignments.

- **Manual Overrides**  
  Edit any assignment directly in the roster and lock assignments to prevent changes.

- **Date Flexibility**  
  Create and view rosters for any month and year.

- **Dynamic Hospital Configuration**  
  Set the number and names of dispensaries, stores, and external postings from the UI.

- **Detailed Analytics**  
  View assignment distribution, night call frequency, and workload balance.

- **Enhanced Validation**  
  Detect duplicate assignments, consecutive night calls, and unassigned shifts before saving.

- **Data Import & Reset**  
  Import pharmacists from CSV/Excel or clear the table to start fresh.

- **Multi-Format Export**  
  Download rosters as Excel (with night shift highlighting) or CSV.

- **Persistent Data**  
  All data is saved in a local SQLite database.

---

## **Installation**

```bash
pip install streamlit pandas xlsxwriter
```

---

## **Quick Start**

1. Install requirements (see above)
2. Run the app:

     ```bash
   streamlit run roster_app.py
   ```

3. Open your browser to [localhost:8501](http://localhost:8501)

---

## **How to Use**

- **Pharmacists Tab:**  
  Add, edit, clear, or import pharmacist data.

- **Roster Tab:**  
  Select the month/year, configure units and postings, and generate the roster.  
  Use "Edit Assignments" to manually override or lock any assignment.

- **Analytics Tab:**  
  Explore assignment statistics, night call frequency, and workload balance.

- **Settings Tab:**  
  Set the number and names of dispensaries, stores, and external postings.

---

## **Database Schema**

- `pharmacists` table: 

  ```sql
  (name TEXT PRIMARY KEY, last_unit TEXT, last_night_call TEXT)
  ```

- `roster_log` table:  

  ```sql
  (month TEXT PRIMARY KEY, roster_data BLOB)
  ```

---

## **Support**

For questions or suggestions, please open an issue or contact the developer.

---
