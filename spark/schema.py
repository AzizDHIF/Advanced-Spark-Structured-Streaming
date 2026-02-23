"""
schema.py — defines the expected schema and all data-quality validation rules.

Validation rules:
  - device_id  : non-null, non-empty string
  - event_time : non-null, parseable timestamp
  - temperature: numeric (not "hot"), not sentinel -999
  - country    : non-null, non-empty, no digits-only, no special chars like @
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType,
)

# ---------------------------------------------------------------------------
# Expected schema for each event
# ---------------------------------------------------------------------------
EVENT_SCHEMA = StructType([
    StructField("device_id",   StringType(), nullable=True),
    StructField("event_time",  StringType(), nullable=True),
    StructField("temperature", DoubleType(), nullable=True),  # "hot" -> null automatically
    StructField("country",     StringType(), nullable=True),
])


# ---------------------------------------------------------------------------
# Validation rule functions — called AFTER Spark session is active
# ---------------------------------------------------------------------------

def _device_id_ok():
    return F.col("device_id").isNotNull() & (F.trim(F.col("device_id")) != "")


def _event_time_ok():
    return (
        F.col("event_time").isNotNull()
        & F.to_timestamp(F.col("event_time")).isNotNull()
    )


def _temperature_ok():
    return (
        F.col("temperature").isNotNull()
        & (F.col("temperature") != -999.0)
    )


def _country_ok():
    return (
        F.col("country").isNotNull()
        & (F.trim(F.col("country")) != "")
        & ~F.col("country").rlike("^[0-9]+$")
        & ~F.col("country").rlike("[^a-zA-Z\\s\\-]")
    )


def _is_valid():
    """Returns a Column expression — only call this inside Spark operations."""
    return _device_id_ok() & _event_time_ok() & _temperature_ok() & _country_ok()


# ---------------------------------------------------------------------------
# Public helpers
# ---------------------------------------------------------------------------

def add_parsed_timestamp(df: DataFrame) -> DataFrame:
    """Cast the string event_time to a proper TimestampType column."""
    return df.withColumn("event_ts", F.to_timestamp(F.col("event_time")))


def split_valid_invalid(df: DataFrame):
    """
    Split a parsed DataFrame into (valid_df, invalid_df).
    invalid_df carries an extra 'invalid_reason' column.
    """
    valid_df = df.filter(_is_valid())

    reason = (
        F.when(~_device_id_ok(),  F.lit("bad_device_id"))
         .when(~_event_time_ok(), F.lit("bad_event_time"))
         .when(~_temperature_ok(),F.lit("bad_temperature"))
         .when(~_country_ok(),    F.lit("bad_country"))
         .otherwise(F.lit("unknown"))
    )

    invalid_df = df.filter(~_is_valid()).withColumn("invalid_reason", reason)

    return valid_df, invalid_df