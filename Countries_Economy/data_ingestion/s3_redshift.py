import psycopg2
import os
import traceback
from dotenv import load_dotenv

# Load .env if needed
load_dotenv()

# Redshift credentials from .env or fallback
REDSHIFT_HOST = os.getenv("REDSHIFT_HOST")
REDSHIFT_PORT = os.getenv("REDSHIFT_PORT")
REDSHIFT_DB = os.getenv("REDSHIFT_DB")
REDSHIFT_USER = os.getenv("REDSHIFT_USER")
REDSHIFT_PASSWORD = os.getenv("REDSHIFT_PASSWORD")
IAM_ROLE = os.getenv("IAM_ROLE") or 'arn:aws:iam::842676012982:role/service-role/AmazonRedshift-CommandsAccessRole-20250208T175217'

print("🔧 Connecting with:")
print(f"🔑 IAM Role: {IAM_ROLE}")
print(f"📡 Host: {REDSHIFT_HOST}, DB: {REDSHIFT_DB}, User: {REDSHIFT_USER}")

S3_BUCKET = "oecd-countries-data"
S3_PREFIX = "csv"

# COPY command templates
copy_commands = {
    "countries": f"""
        COPY countries
        FROM 's3://{S3_BUCKET}/{S3_PREFIX}/countries.csv'
        IAM_ROLE '{IAM_ROLE}'
        FORMAT AS CSV
        IGNOREHEADER 1;
    """,
    "economies": f"""
        COPY economies
        FROM 's3://{S3_BUCKET}/{S3_PREFIX}/economies.csv'
        IAM_ROLE '{IAM_ROLE}'
        FORMAT AS CSV
        IGNOREHEADER 1;
    """,
    "cities": f"""
        COPY cities
        FROM 's3://{S3_BUCKET}/{S3_PREFIX}/cities.csv'
        IAM_ROLE '{IAM_ROLE}'
        FORMAT AS CSV
        IGNOREHEADER 1;
    """,
    "population": f"""
        COPY population
        FROM 's3://{S3_BUCKET}/{S3_PREFIX}/populations.csv'
        IAM_ROLE '{IAM_ROLE}'
        FORMAT AS CSV
        IGNOREHEADER 1;
    """,
    "languages": f"""
        COPY languages
        FROM 's3://{S3_BUCKET}/{S3_PREFIX}/languages.csv'
        IAM_ROLE '{IAM_ROLE}'
        FORMAT AS CSV
        IGNOREHEADER 1;
    """,
    "currencies": f"""
        COPY currencies
        FROM 's3://{S3_BUCKET}/{S3_PREFIX}/currencies.csv'
        IAM_ROLE '{IAM_ROLE}'
        FORMAT AS CSV
        IGNOREHEADER 1;
    """
}

def load_data():
    try:
        conn = psycopg2.connect(
            host=REDSHIFT_HOST,
            port=REDSHIFT_PORT,
            dbname=REDSHIFT_DB,
            user=REDSHIFT_USER,
            password=REDSHIFT_PASSWORD
        )
        cur = conn.cursor()

        for table, command in copy_commands.items():
            print(f"\n📥 Loading data into: {table}")
            print(f"📤 COPY command:\n{command.strip()}")

            try:
                cur.execute(command)
                conn.commit()
                print(f"✅ Data loaded into: {table}")
            except Exception as copy_error:
                conn.rollback()
                print(f"❌ Error loading table '{table}': {copy_error}")
                traceback.print_exc()

                # Optional: diagnose error from stl_load_errors
                try:
                    cur.execute("""
                        SELECT starttime, filename, line_number, position, raw_line, err_reason
                        FROM stl_load_errors
                        WHERE tbl = (
                            SELECT oid FROM pg_table_def
                            WHERE tablename = %s
                            LIMIT 1
                        )
                        ORDER BY starttime DESC
                        LIMIT 1;
                    """, (table,))

                    err = cur.fetchone()
                    if err:
                        print("\n📛 Redshift load error details:")
                        print(f"🕒 Time       : {err[0]}")
                        print(f"📄 File       : {err[1]}")
                        print(f"📍 Line       : {err[2]} (pos {err[3]})")
                        print(f"📝 Raw line   : {err[4]}")
                        print(f"❗ Reason     : {err[5]}")
                except Exception as diag_error:
                    print(f"⚠️ Failed to fetch load error info: {diag_error}")

        cur.close()
        conn.close()
        print("\n🎉 All COPY operations attempted.")

    except Exception as e:
        print(f"❌ Failed to connect or execute COPY: {e}")
        traceback.print_exc()

if __name__ == "__main__":
    load_data()
