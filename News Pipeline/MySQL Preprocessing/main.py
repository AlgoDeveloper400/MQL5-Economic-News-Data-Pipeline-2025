import os
import time
import mysql.connector
import pandas as pd
from datetime import datetime
import glob
import math
import re
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv(r"C:/Users/Map/your/own/volume/here\.env")

def wait_for_mysql_startup(host=None, user=None, password=None, database=None, max_wait=120):
    """Wait until MySQL is ready to accept connections."""
    # Get credentials from environment variables with fallbacks
    host = host or os.getenv("MYSQL_HOST", "mysql")
    user = user or "root"  # Always use root as requested
    password = password or os.getenv("MYSQL_ROOT_PASSWORD")
    database = database or os.getenv("MYSQL_DATABASE", "forex_events")
    
    if not password:
        raise ValueError("❌ MYSQL_ROOT_PASSWORD not found in environment variables")
    
    print("⏳ Waiting for MySQL to start...", flush=True)
    start_time = time.time()
    while True:
        try:
            conn = mysql.connector.connect(
                host=host,
                user=user,
                password=password,
                database=database
            )
            conn.close()
            print("✅ MySQL is ready!", flush=True)
            return
        except mysql.connector.Error:
            if time.time() - start_time > max_wait:
                raise TimeoutError("❌ MySQL did not start in time.")
            time.sleep(5)

def find_csv_file(data_folder):
    csv_files = glob.glob(os.path.join(data_folder, "*.csv"))
    if not csv_files:
        raise FileNotFoundError(f"No CSV files found in {data_folder}")
    return csv_files[0]

def parse_date(date_str):
    """Parse date from various formats and return as datetime.date object"""
    try:
        if pd.isna(date_str) or str(date_str).strip() == "":
            return None
        
        date_str = str(date_str).strip()
        
        # Handle '2007-01-10' format (MySQL DATE format)
        if re.match(r'^\d{4}-\d{2}-\d{2}$', date_str):
            return datetime.strptime(date_str, "%Y-%m-%d").date()
        
        # Handle '10 January 2007' format
        months = ["January", "February", "March", "April", "May", "June",
                 "July", "August", "September", "October", "November", "December"]
        if any(month in date_str for month in months):
            # Try different day formats (with/without leading zero)
            try:
                return datetime.strptime(date_str, "%d %B %Y").date()
            except ValueError:
                try:
                    return datetime.strptime(date_str, "%e %B %Y").date()  # %e for day without leading zero
                except ValueError:
                    pass
        
        # Handle other common date formats
        formats_to_try = [
            "%m/%d/%Y", "%d/%m/%Y", "%Y/%m/%d",
            "%m-%d-%Y", "%d-%m-%Y", "%Y-%m-%d",
            "%b %d, %Y", "%B %d, %Y"
        ]
        
        for fmt in formats_to_try:
            try:
                return datetime.strptime(date_str, fmt).date()
            except ValueError:
                continue
        
        print(f"⚠️ Could not parse date: {date_str}")
        return None
        
    except Exception as e:
        print(f"❌ Error parsing date '{date_str}': {e}")
        return None

def parse_time(time_str):
    """Parse time from various formats and return as datetime.time object"""
    try:
        if pd.isna(time_str) or str(time_str).strip() == "":
            return None
        time_str = str(time_str).strip()
        
        # Handle '14:30' format
        if re.match(r'^\d{1,2}:\d{2}$', time_str):
            return datetime.strptime(time_str, "%H:%M").time()
        
        # Handle '2:30 PM' format
        if 'AM' in time_str.upper() or 'PM' in time_str.upper():
            try:
                time_obj = datetime.strptime(time_str, "%I:%M %p").time()
                return time_obj
            except ValueError:
                pass
        
        # Handle 24-hour format without leading zero
        if re.match(r'^\d{1,2}:\d{2}$', time_str):
            parts = time_str.split(':')
            if len(parts) == 2:
                hour = int(parts[0])
                minute = int(parts[1])
                if 0 <= hour <= 23 and 0 <= minute <= 59:
                    return datetime.strptime(f"{hour:02d}:{minute:02d}", "%H:%M").time()
        
        print(f"⚠️ Could not parse time: {time_str}")
        return None
        
    except Exception as e:
        print(f"❌ Error parsing time '{time_str}': {e}")
        return None

def clean_text_value(value):
    """Clean and standardize text values"""
    if pd.isna(value) or str(value).strip() == "":
        return "N/A"
    
    value_str = str(value).strip()
    
    # Remove extra whitespace and normalize empty values
    if value_str in ["", "nan", "None", "null", "NULL"]:
        return "N/A"
    
    return value_str

def import_csv_data():
    # Get database configuration from environment variables
    db_config = {
        "host": os.getenv("MYSQL_HOST", "mysql"),
        "user": "root",  # Always use root as requested
        "password": os.getenv("MYSQL_ROOT_PASSWORD"),
        "database": os.getenv("MYSQL_DATABASE", "forex_events"),
    }

    # Validate that we have the root password
    if not db_config["password"]:
        raise ValueError("❌ MYSQL_ROOT_PASSWORD environment variable is required")

    data_folder = os.getenv("DATA_FOLDER", "/csv_data")

    try:
        # Wait for MySQL
        wait_for_mysql_startup(
            host=db_config["host"],
            user=db_config["user"],
            password=db_config["password"],
            database=db_config["database"],
        )

        # Load CSV
        csv_file = find_csv_file(data_folder)
        print(f"📂 Loading CSV file: {csv_file}", flush=True)

        df = pd.read_csv(
            csv_file,
            header=None,
            names=[
                "Date", "Time", "Currency", "Event", "Impact",
                "Actual", "Forecast", "Previous", "IsHoliday", "WeekRange"
            ],
            quotechar='"',
            skipinitialspace=True,
            na_filter=False,
            low_memory=False
        )

        # Drop unwanted columns
        for col in ["IsHoliday", "WeekRange"]:
            if col in df.columns:
                df.drop(columns=[col], inplace=True)

        print(f"📊 Original dataset size: {len(df)} rows")

        # Clean and parse data
        print("🔄 Parsing dates and times...")
        df["Date"] = df["Date"].apply(parse_date)
        df["Time"] = df["Time"].apply(parse_time)
        
        # Clean text columns
        for col in ["Currency", "Event", "Impact", "Actual", "Forecast", "Previous"]:
            df[col] = df[col].apply(clean_text_value)

        # Remove rows with invalid dates or times
        initial_count = len(df)
        df = df.dropna(subset=["Date", "Time"])
        removed_count = initial_count - len(df)
        if removed_count > 0:
            print(f"⚠️ Removed {removed_count} rows with invalid dates/times")

        if df.empty:
            print("❌ No valid data remaining after cleaning")
            return

        # Merge Date and Time for sorting and incremental filtering
        df["DateTime"] = pd.to_datetime(
            df["Date"].astype(str) + " " + df["Time"].astype(str), 
            errors="coerce"
        )
        df = df.dropna(subset=["DateTime"])
        df = df.sort_values(by="DateTime", ascending=True).reset_index(drop=True)

        # Connect to MySQL
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()

        # Ensure table exists with unique key (same as initialization script)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS events (
                id INT AUTO_INCREMENT PRIMARY KEY,
                Date DATE NOT NULL,
                Time TIME NOT NULL,
                Currency VARCHAR(10) NOT NULL,
                Event VARCHAR(255) NOT NULL,
                Impact VARCHAR(20),
                Actual VARCHAR(50),
                Forecast VARCHAR(50),
                Previous VARCHAR(50),
                UNIQUE KEY unique_event_key (Date, Time, Currency, Event)
            )
        """)
        
        # Create the formatted view
        cursor.execute("""
            CREATE OR REPLACE VIEW events_formatted AS
            SELECT 
                id,
                DATE_FORMAT(Date, '%e %M %Y') as Date,
                Time,
                Currency,
                Event,
                Impact,
                Actual,
                Forecast,
                Previous
            FROM events
        """)
        
        connection.commit()

        # Smart merge: filter only new rows based on last timestamp
        cursor.execute("SELECT MAX(CONCAT(Date, ' ', Time)) FROM events")
        last_timestamp = cursor.fetchone()[0]

        if last_timestamp:
            last_dt = pd.to_datetime(last_timestamp)
            df = df[df["DateTime"] > last_dt]
            print(f"🕒 Filtering for data after: {last_dt}")

        if df.empty:
            print("ℹ️ No new data to import.", flush=True)
            return

        # Drop DateTime before inserting into DB
        df = df.drop(columns=["DateTime"])

        # Prepare insert
        insert_query = """
        INSERT INTO events (Date, Time, Currency, Event, Impact, Actual, Forecast, Previous)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            Impact = VALUES(Impact),
            Actual = VALUES(Actual),
            Forecast = VALUES(Forecast),
            Previous = VALUES(Previous)
        """

        data_to_insert = [
            (
                row["Date"], row["Time"], row["Currency"], row["Event"],
                row["Impact"], row["Actual"], row["Forecast"], row["Previous"]
            )
            for _, row in df.iterrows()
        ]

        total = len(data_to_insert)
        chunk_size = 50
        last_progress = -1

        print(f"📤 Importing {total} new records...")

        for i in range(0, total, chunk_size):
            chunk = data_to_insert[i:i+chunk_size]
            cursor.executemany(insert_query, chunk)
            connection.commit()

            progress = math.floor(((i + len(chunk)) / total) * 100)
            if progress != last_progress:
                print(f"📈 Progress: {progress}%", flush=True)
                last_progress = progress

        print(f"✅ Smart merge complete. {total} new records processed in ascending order.", flush=True)
        
        # Display sample of imported data
        cursor.execute("SELECT Date, Time, Currency, Event FROM events_formatted ORDER BY id DESC LIMIT 5")
        sample_data = cursor.fetchall()
        print("📋 Sample of imported data (with formatted dates):")
        for row in sample_data:
            print(f"   {row[0]} {row[1]} - {row[2]}: {row[3]}")

    except Exception as e:
        print(f"❌ Data import failed: {e}", flush=True)
        import traceback
        traceback.print_exc()

    finally:
        if "cursor" in locals():
            cursor.close()
        if "connection" in locals():
            connection.close()

if __name__ == "__main__":
    import_csv_data()