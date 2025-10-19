
# Data Ingestion Demo using the options
- Spark Dataframe Write
- CTAS(Create Table As Select)
- Spark SQL

## Overview
This project demonstrates a **complete data ingestion workflow** using **Apache Spark 3.5.x**, **Scala 2.13**, and **Delta Lake 3.3.0**

It is designed for educational purposes to showcase the data ingestion techniques for the beginners

## Source File Formats
- CSV
- JSON
- Parquet

## Target File Format
- Delta Table

## Table of Contents
1. Project Structure
2. Prerequisites
3. Setup
4. Running the Demo
5. Synthetic Sample Data
6. Project Highlights
7. Notes & Best Practices

## Project Structure
```
data_engineering_e2e/
│
├─ src/main/scala/
│   ├─ ingestion
│   │   └─ DataIngestionMain
├─ src/main/resources/
│   ├─ ingestion
│   ├─ data
│   │   └─ input
│   │       └─ patients.csv
│   │       └─ patient_visits.json
│   │       └─ patient_diagnosis.parquet
│   │   └─ output
│   │       └─ delta_tables
│   │           └─ patients
│   │           └─ patients_visits
│   │           └─ patients_diagnosis
├─ pom.xml
├─ README.md
```

## Prerequisites
- Java 11+
- Maven 3.8+
- IntelliJ IDEA or other IDE
- Apache Spark 3.5.x
- Delta Lake 3.3.0
- Scala 2.13.x

## Setup
1. Clone the repository:
```bash
git clone https://github.com/shahul-bathusha/data-engineering-hands-on.git
cd data_engineering_e2e
```
2. Build the project:
```bash
mvn clean install
```
3. Add Spark configuration in IDE (optional)

## Running the Demo
```bash
$SPARK_HOME/bin/spark-submit   --class ingestion.DataIngestionMain  --packages io.delta:delta-spark_2.13:3.3.1 target/data_engineering_e2e-1.0-SNAPSHOT-jar-with-dependencies.jar
```


## Synthetic Sample Data
**patients.csv**: patient_id, name, age, gender  
**visits.json**: visit_id, patient_id, date  
**diagnosis.parquet**: visit_id, diagnosis_name

