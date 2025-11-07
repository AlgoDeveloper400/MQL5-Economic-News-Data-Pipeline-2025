from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, concat_ws, to_timestamp, trim
import os
import sys

# -----------------------------
# CONFIG - Using container paths that map to your Windows directories
# -----------------------------
input_path = Path("/app/input")  # Maps to your "Merged Batch" folder
output_path = Path("/app/output") # Maps to your "Arranged Batch" folder
output_path.mkdir(exist_ok=True)  # create folder if not exists

print("=== Configuration ===")
print(f"Input path (container): {input_path}")
print(f"Output path (container): {output_path}")
print(f"Input directory exists: {input_path.exists()}")
print(f"Output directory exists: {output_path.exists()}")

if input_path.exists():
    print(f"Files in input directory: {list(input_path.glob('*'))}")

# -----------------------------
# CREATE SPARK SESSION with optimized configuration
# -----------------------------
spark = SparkSession.builder \
    .appName("CSV DateTime Sort and Save") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "2g") \
    .config("spark.driver.cores", "2") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .getOrCreate()

# Set log level to reduce verbosity
spark.sparkContext.setLogLevel("WARN")

print("Spark session created successfully")

try:
    # -----------------------------
    # PROCESS LATEST CSV
    # -----------------------------
    csv_files = list(input_path.glob("*.csv"))
    print(f"Found {len(csv_files)} CSV files in input directory")
    
    if not csv_files:
        print("No CSV files found in input directory.")
        print(f"Available files in {input_path}: {list(input_path.glob('*'))}")
        sys.exit(1)
    else:
        # Get the most recently modified CSV file
        csv_file = max(csv_files, key=lambda f: f.stat().st_mtime)
        print(f"Processing file: {csv_file.name}")
        print(f"File path: {csv_file}")

        # Read CSV with explicit schema inference
        df = spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .option("encoding", "UTF-8") \
            .option("mode", "PERMISSIVE") \
            .csv(str(csv_file))

        print(f"Original DataFrame count: {df.count()}")
        print(f"DataFrame columns: {df.columns}")

        # Trim all string columns
        for column in df.columns:
            df = df.withColumn(column, trim(col(column)))

        # Clean Time column
        df = df.withColumn("Time", regexp_replace(col("Time"), "(?i)all\\s*day", "00:00"))
        df = df.withColumn("Time", regexp_replace(col("Time"), r"^(\d{1,2}:\d{2}).*", r"$1"))

        # Create sortable DateTimeTemp
        df = df.withColumn("DateTimeTemp", concat_ws(" ", col("Date"), col("Time")))
        df = df.withColumn("DateTimeTemp", to_timestamp(col("DateTimeTemp"), "d MMMM yyyy HH:mm"))

        # Filter rows that failed parsing
        initial_count = df.count()
        df = df.filter(col("DateTimeTemp").isNotNull())
        filtered_count = df.count()
        
        print(f"Rows after datetime filtering: {filtered_count}/{initial_count}")

        if filtered_count == 0:
            print("No valid rows after datetime parsing. Check your date/time formats.")
            sys.exit(1)
        else:
            # Sort ascending
            df_sorted = df.orderBy(col("DateTimeTemp").asc())

            # Drop temp column
            df_sorted = df_sorted.drop("DateTimeTemp")

            # -----------------------------
            # CONVERT TO PANDAS AND SAVE
            # -----------------------------
            print("Converting to Pandas DataFrame...")
            df_pandas = df_sorted.toPandas()

            # Construct output file name
            original_name = csv_file.stem  # filename without extension
            output_file = output_path / f"{original_name}_arranged.csv"

            # -----------------------------
            # DELETE ANY EXISTING CSV FILES FIRST
            # -----------------------------
            existing_csvs = list(output_path.glob("*.csv"))
            print(f"Found {len(existing_csvs)} existing CSV files in output directory")
            
            for old_file in existing_csvs:
                try:
                    os.remove(old_file)
                    print(f"Deleted old file: {old_file.name}")
                except Exception as e:
                    print(f"Could not delete {old_file.name}: {e}")

            # Save CSV
            print(f"Saving to: {output_file}")
            df_pandas.to_csv(output_file, index=False, encoding="utf-8")
            print(f"✓ Processed CSV saved to: {output_file}")
            print(f"✓ Output file size: {output_file.stat().st_size} bytes")

            # Verify the file was created
            if output_file.exists():
                print("✓ Output file verified successfully")
            else:
                print("✗ Output file was not created!")
                sys.exit(1)

            # Optional: show top 50 rows
            print("\nFirst 5 rows of processed data:")
            print(df_pandas.head(5))

            print("=== Processing completed successfully ===")

except Exception as e:
    print(f"❌ Error during processing: {str(e)}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

finally:
    # Stop Spark session
    spark.stop()
    print("Spark session stopped")