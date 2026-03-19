"""
House Sale Data ETL Pipeline
============================
ETL Steps:
  1. EXTRACT  – load CSV into PySpark DataFrame
  2. TRANSFORM – split by neighborhood and save CSVs
  3. LOAD      – write each neighborhood to PostgreSQL
"""
from __future__ import annotations

import csv
import os
import shutil
from pathlib import Path

from dotenv import load_dotenv
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

# -------------------------------------------------------------------
# CONSTANTS (DO NOT MODIFY)
# -------------------------------------------------------------------

ROOT = Path(__file__).resolve().parent.parent

NEIGHBORHOODS = [
    "Downtown", "Green Valley", "Hillcrest", "Lakeside", "Maple Heights",
    "Oakwood", "Old Town", "Riverside", "Suburban Park", "University District",
]

OUTPUT_DIR = ROOT / "output" / "by_neighborhood"

OUTPUT_FILES = {
    hood: OUTPUT_DIR / f"{hood.replace(' ', '_').lower()}.csv"
    for hood in NEIGHBORHOODS
}

PG_TABLES = {
    hood: f"public.{hood.replace(' ', '_').lower()}"
    for hood in NEIGHBORHOODS
}

# -------------------------------------------------------------------
# 1. EXTRACT
# -------------------------------------------------------------------

def extract(spark: SparkSession, csv_path: str) -> DataFrame:
    """Load CSV into DataFrame"""
    return spark.read.csv(csv_path, header=True, inferSchema=True)

# -------------------------------------------------------------------
# 2. TRANSFORM
# -------------------------------------------------------------------

def transform(df: DataFrame) -> dict[str, DataFrame]:
    """
    Split dataset by neighborhood and save CSVs
    (single file per neighborhood)
    """
    partitions: dict[str, DataFrame] = {}
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    for hood in NEIGHBORHOODS:
        # Filter and sort deterministically for CI tests
        hood_df = (
            df.filter(F.col("neighborhood") == hood)
              .orderBy("house_id")
        )

        partitions[hood] = hood_df

        if hood_df.limit(1).count() > 0:
            temp_dir = str(OUTPUT_FILES[hood]) + "_tmp"

            # Write Spark output (folder)
            hood_df.coalesce(1).write.csv(
                temp_dir,
                header=True,
                mode="overwrite"
            )

            # Move the actual CSV file out of Spark folder
            for file in os.listdir(temp_dir):
                if file.startswith("part-") and file.endswith(".csv"):
                    shutil.move(
                        os.path.join(temp_dir, file),
                        OUTPUT_FILES[hood]
                    )

            # Delete temp folder
            shutil.rmtree(temp_dir)

    return partitions

# -------------------------------------------------------------------
# 3. LOAD
# -------------------------------------------------------------------

def load(partitions: dict[str, DataFrame], jdbc_url: str, pg_props: dict) -> None:
    """Load each neighborhood into PostgreSQL"""
    for hood, hood_df in partitions.items():
        table_name = PG_TABLES[hood]
        if hood_df.limit(1).count() > 0:
            hood_df.write.jdbc(
                url=jdbc_url,
                table=table_name,
                mode="overwrite",
                properties=pg_props
            )

# -------------------------------------------------------------------
# MAIN (DO NOT MODIFY)
# -------------------------------------------------------------------

def main() -> None:
    load_dotenv(ROOT / ".env")

    jdbc_url = (
        f"jdbc:postgresql://{os.getenv('PG_HOST', 'localhost')}:"
        f"{os.getenv('PG_PORT', '5432')}/{os.environ['PG_DATABASE']}"
    )

    pg_props = {
        "user": os.environ["PG_USER"],
        "password": os.getenv("PG_PASSWORD", ""),
        "driver": "org.postgresql.Driver",
    }

    # FIXED: use the correct dataset filename
    csv_path = str(
        ROOT
        / os.getenv("DATASET_DIR", "dataset")
        / os.getenv("DATASET_FILE", "historical_purchases.csv")
    )

    spark = (
        SparkSession.builder.appName("HouseSaleETL")
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    df = extract(spark, csv_path)
    partitions = transform(df)
    load(partitions, jdbc_url, pg_props)

    spark.stop()


if __name__ == "__main__":
    main()
