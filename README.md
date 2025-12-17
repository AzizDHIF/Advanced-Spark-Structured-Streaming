# Lab 2 — Advanced Spark Structured Streaming  
## Time, State, and Data Quality

---

## Context
In Lab 1, you learned how to connect Kafka to Spark Structured Streaming and apply basic transformations.

In this lab, you will work with **structured but dirty streaming data** and explore the **core challenges of real-world stream processing**:
state, event time, windowing, watermarks, and data quality.

---

## Prerequisites
You must have:
- Docker installed and running
- Kafka running via Docker
- Apache Spark available (local or Docker)
- Python 3.9+

---

## Architecture
Kafka (JSON events) → Spark Structured Streaming → Validation / Aggregation → Output


Kafka handles **ingestion and durability**.  
Spark handles **structure, time, and state**.

---

## Data

The dataset may include invalid JSON, missing fields, wrong types, and inconsistent values.

⚠️ You must not modify the input data.

---

## Handle Dirty Data
Separate the stream into:
- **Valid events** (correct schema and values)
- **Invalid events** (malformed, missing, or incorrect data)

Requirements:
- The application must not crash
- Invalid events must be isolated (e.g., separate sink or topic)

---

## Event Time and Watermarks
- Use event time instead of processing time
- Define a watermark (e.g. 10 minutes)
- Observe how late data is handled

Key idea: state must be bounded in infinite streams.

---

## Windowed Aggregations
Perform stateful aggregations, such as:
- Average temperature per device per time window
- Event count per country per window

Use event-time windows.

---

## Write Streaming Results
Write results to one or more sinks:
- Console (for debugging)
- File sink (Parquet or JSON)
- Kafka topic

Experiment with output modes:
- `append`
- `update`
- `complete`

---

## Expected Outcome
At the end of this lab, you should have:
- A robust Spark Structured Streaming application
- Correct handling of dirty streaming data
- Stateful, windowed aggregations
- A clear understanding of time and state in streams

If your application crashes on bad data, the lab is **not complete**.
