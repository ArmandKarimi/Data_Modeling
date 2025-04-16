# 🌍 OECD Countries Economy – Data Modeling Practice

This project is a **personal SQL and data modeling practice** focused on working with real-world country-level data, inspired by OECD-style datasets.

The purpose of this exercise is to **strengthen foundational skills** in:

- ✅ SQL joins, aggregations, and filtering
- ✅ Database design and schema modeling
- ✅ Understanding and applying primary keys & foreign keys
- ✅ Loading data from **AWS S3** to **Amazon Redshift**
- ✅ Using tools like **DBeaver**, **Jupyter**, and **Python (psycopg2, boto3)**


## 📦 Data Overview
We work with 6 CSV files (⚠️ Note: These CSVs are provided for learning purposes and may not be 100% accurate or up-to-date. Always consult the original source for official use.)

- `countries.csv` – country-level metadata (name, region, capital, etc.)
- `economies.csv` – GDP, inflation, unemployment, etc.
- `population.csv` – fertility, life expectancy, and population
- `cities.csv` – population stats for major cities
- `languages.csv` – spoken languages and percentages
- `currencies.csv` – currency details per country

Each table was carefully modeled to:

- Identify **primary keys** (e.g. `(code, year)` or `(name, country_code)`)
- Establish **foreign key relationships** with the main `countries` table
- Handle data types like `float`, `int`, and `boolean`

## 🚀 Setup & Usage
```bash
cd data_ingestion
### 1. Create tables in Redshift
python create_tables.py
### 2. Load data to S3 bucket
python load_s3.py
### 3. Load data from S3 bucket to Redshift
python s3_redshift.py
```
## 🛠️ Technologies Used
- Amazon Redshift – cloud data warehouse
- mazon S3 – for storing and staging CSV data
- Python – data ingestion using psycopg2 and boto3
- SQL – querying, joins, aggregations, and analysis
- DBeaver – GUI for exploring and testing SQL queries
- Jupyter Notebook – exploration, analysis, and testing