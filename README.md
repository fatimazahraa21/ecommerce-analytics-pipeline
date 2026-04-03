# E-commerce Analytics Pipeline

## 📊 Project Overview
A complete data engineering pipeline for e-commerce analysis using PySpark, PostgreSQL, and advanced SQL.

**Data:** 500K+ e-commerce transactions  
**Stack:** PySpark | PostgreSQL | SQL Advanced | Python

## 🎯 Objectives
- Extract and load e-commerce data using PySpark
- Clean and process data in Spark
- Store in PostgreSQL
- Analyze with advanced SQL (Window Functions, CTEs)
- Generate insights and reports

## 🛠️ Tech Stack
- **PySpark** - ETL and data processing
- **PostgreSQL** (Neon) - Data warehouse
- **SQL** - Advanced queries (Window Functions, CTEs)
- **Python 3.9+** - Data manipulation
- **Jupyter** - Notebooks for exploration

## 📂 Project Structure
ecommerce-analytics-pipeline/
├── README.md                 # This file
├── requirements.txt          # Python dependencies
│
├── data/
│   ├── raw/                  # Original CSV file
│   └── processed/            # Processed data (from Spark)
│
├── notebooks/
│   ├── 01_data_exploration.ipynb      # EDA
│   ├── 02_pyspark_etl.ipynb           # ETL process
│   └── 03_sql_analysis.ipynb          # SQL queries
│
├── sql/
│   ├── 01_create_tables.sql           # Table creation
│   ├── 02_window_functions.sql        # Window Functions
│   ├── 03_cte_analysis.sql            # CTEs
│   └── 04_final_insights.sql          # Key findings
│
└── results/
├── analysis_report.md              # Final report
├── top_customers.csv               # Results
└── revenue_by_month.csv            # Results

## 🚀 How to Run

### 1. Load Data
```python
import pandas as pd
df = pd.read_csv("data/raw/ecommerce_data.csv")
```

### 2. Run PySpark ETL
See `notebooks/02_pyspark_etl.ipynb`

### 3. Run SQL Queries
See `sql/` folder for all queries

### 4. View Results
Check `results/analysis_report.md`

## 📊 Key Findings
- [To be updated after analysis]

## 🎓 Skills Demonstrated
✅ PySpark ETL Pipeline  
✅ PostgreSQL Data Warehouse  
✅ Advanced SQL (Window Functions, CTEs, JOINs)  
✅ Data Analysis & Insights  
✅ Python Data Manipulation  

## 📝 License
MIT License - See LICENSE file

## 👤 Author
Fatima Zahra - Data Engineer
