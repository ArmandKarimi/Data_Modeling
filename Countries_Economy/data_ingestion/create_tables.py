import psycopg2
import os
from dotenv import load_dotenv

# Get the root project directory (bitcoin-model-regression-nextday)
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../"))

# Debugging: Print the expected .env path
env_path = os.path.join(BASE_DIR, ".env")

# Load the .env file from the root directory
load_dotenv(env_path)

# Get Redshift credentials
IAM_ROLE = os.getenv("IAM_ROLE")  # For IAM authentication
REDSHIFT_HOST = os.getenv("REDSHIFT_HOST")
REDSHIFT_PORT = os.getenv("REDSHIFT_PORT")
REDSHIFT_DB = os.getenv("REDSHIFT_DB")
REDSHIFT_USER = os.getenv("REDSHIFT_USER")
REDSHIFT_PASSWORD = os.getenv("REDSHIFT_PASSWORD")


# function to create the table
def create_table(create_table_query):
    try:
        conn = psycopg2.connect(
            host=REDSHIFT_HOST,
            port=REDSHIFT_PORT,
            dbname=REDSHIFT_DB,
            user=REDSHIFT_USER,
            password=REDSHIFT_PASSWORD
        )
        
        cur = conn.cursor()
        cur.execute(create_table_query)
        conn.commit()
        
        print("✅ Table created successfully!")
        
        cur.close()
        conn.close()
    except Exception as e:
        print(f"❌ Error creating table: {e}")


#`function to set up the schema`
def setup_schema():
    try:
        with psycopg2.connect(
            host=REDSHIFT_HOST,
            port=REDSHIFT_PORT,
            dbname=REDSHIFT_DB,
            user=REDSHIFT_USER,
            password=REDSHIFT_PASSWORD
        ) as conn:
            with conn.cursor() as cur:
                cur.execute("CREATE SCHEMA IF NOT EXISTS oecd;")
                cur.execute("SET search_path TO oecd;")
                print("✅ Schema 'oecd' created and search_path set.")
    except Exception as e:
        print(f"❌ Error setting up schema: {e}")

# Note: The IAM role should have the necessary permissions to create schemas in the Redshift database.
# Note: The search_path is set to the 'oecd' schema for subsequent queries.
# Note: The schema name 'oecd' is used as per the requirement.

if __name__ == "__main__":
    create_countries_table_query = """
        CREATE TABLE IF NOT EXISTS oecd.countries (
        code varchar(3) NOT NULL PRIMARY KEY,
        country_name varchar(255),
        continent varchar(255),
        region varchar(255),
        surface_area float,
        indep_year int,
        local_name varchar(255),
        gov_form varchar(255),
        capital varchar(255),
        cap_long float,
        cap_lat float
        )
        """
    # economy_columns = ['econ_id', 'code', 'year', 'income_group', 'gdp_percapita','gross_savings', 'inflation_rate', 'total_investment','unemployment_rate', 'exports', 'imports']

    create_economies_table_query = """
        CREATE TABLE IF NOT EXISTS oecd.economies (
        econ_id int IDENTITY(1,1) NOT NULL,
        code varchar(3) NOT NULL,
        year int NOT NULL,
        income_group varchar(255),
        gdp_percapita float,
        gross_savings float,
        inflation_rate float,   
        total_investment float,
        unemployment_rate float,
        exports float,
        imports float,
        PRIMARY KEY (code, year),
        FOREIGN KEY (code) REFERENCES oecd.countries(code)
        )
        """
    # Note: The econ_id is assumed to be a unique identifier for each record in the economy table.
    # Note: The code column in the economy table is a foreign key referencing the code column in the countries table.
    # Note: The PRIMARY KEY constraint ensures that each combination of code and year is unique in the economy table.


    # cities_columns = ['name', 'country_code', 'city_proper_pop', 'metroarea_pop','urbanarea_pop']

    create_cities_table_query = """
        CREATE TABLE IF NOT EXISTS oecd.cities (
        name varchar(255) NOT NULL,
        country_code varchar(3) NOT NULL,
        city_proper_pop int,
        metroarea_pop float,
        urbanarea_pop int,
        PRIMARY KEY (name, country_code),
        FOREIGN KEY (country_code) REFERENCES oecd.countries(code)
        )
        """
    
    # Note: The name column is assumed to be a unique identifier for each city in the cities table.
    # Note: The country_code column in the cities table is a foreign key referencing the code column in the countries table.
    # Note: The PRIMARY KEY constraint ensures that each combination of name and country_code is unique in the cities table.
   
    # population_columns = ['pop_id', 'country_code', 'year', 'fertility_rate', 'life_expectancy','size']
    create_population_table_query = """
    CREATE TABLE IF NOT EXISTS oecd.population (
    pop_id int IDENTITY(1,1) NOT NULL,
    country_code varchar(3) NOT NULL,
    year int NOT NULL,
    fertility_rate float,
    life_expectancy float,
    size int,
    PRIMARY KEY (country_code, year),
    FOREIGN KEY (country_code) REFERENCES oecd.countries(code)
    )
    """
    # Note: The pop_id is assumed to be a unique identifier for each record in the population table.
    # Note: The country_code column in the population table is a foreign key referencing the code column in the countries table.
    # Note: The PRIMARY KEY constraint ensures that each combination of country_code and year is unique in the population table.

    # langugae_columns = ['lang_id', 'code', 'name', 'percent', 'official']
    create_language_table_query = """
    CREATE TABLE IF NOT EXISTS oecd.languages (
    lang_id int IDENTITY(1,1) NOT NULL,
    code varchar(3) NOT NULL,
    name varchar(255) NOT NULL,
    language_percent float,
    official boolean,
    PRIMARY KEY (code, name),
    FOREIGN KEY (code) REFERENCES oecd.countries(code)
    )
    """
    # Note: The lang_id is assumed to be a unique identifier for each record in the languages table.        
    # Note: The code column in the languages table is a foreign key referencing the code column in the countries table.
    # Note: The PRIMARY KEY constraint ensures that each combination of code and name is unique in the languages table.
    # Note: The official column is assumed to be a string indicating whether the language is official or not.

    # currency_columns = ['curr_id', 'code', 'basic_unit', 'curr_code', 'frac_unit','frac_perbasic']
    create_currency_table_query = """
    CREATE TABLE IF NOT EXISTS oecd.currencies (
    curr_id int IDENTITY(1,1) NOT NULL,
    code varchar(3) NOT NULL,
    basic_unit varchar(255),
    curr_code varchar(3),
    frac_unit varchar(255),
    frac_perbasic float,
    PRIMARY KEY (code, curr_code),
    FOREIGN KEY (code) REFERENCES oecd.countries(code)
    )
    """
    # Note: The curr_id is assumed to be a unique identifier for each record in the currencies table.   
    # Note: The code column in the currencies table is a foreign key referencing the code column in the countries table.
    # Note: The PRIMARY KEY constraint ensures that each combination of code and curr_code is unique in the currencies table.
 
    # Optional: Set up schema
    setup_schema()

    # Create the tables
    table_queries = [
        create_countries_table_query,
        create_economies_table_query,
        create_cities_table_query,
        create_population_table_query,
        create_language_table_query,
        create_currency_table_query,
                    ]

    for query in table_queries:
        create_table(query)
        print("✅ Table created successfully.")














