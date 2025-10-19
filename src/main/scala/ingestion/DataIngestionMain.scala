package ingestion

import org.apache.spark.sql.{DataFrame, SparkSession}

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

object DataIngestionMain {

  def main(args: Array[String]): Unit = {

    /** Spark session needs to be established to get the spark context
     * This initialization is not required when we execute this in Azure Fabric or Databricks
     * As those platforms compute is already initialized and spark context is readily available */
    val spark = initializeSparkSession()

    /** Read Csv and write it to delta table using spark dataframe write */
    val patientsDf = spark.read.option("header", "true")
      .csv("src/main/resources/ingestion/data/input/patients.csv")
    createDeltaUsingSparkWrite(patientsDf)

    /** Read JSON and write it to delta table using CTAS */
    val patientVisitsDf = spark.read.option("multiLine", true)
      .json("src/main/resources/ingestion/data/input/patient_visits.json")
    createDeltaUsingCTAS(spark,patientVisitsDf)

    /** Read Parquet and write it to delta table using Spark SQL */
    val patientDiagnosisDf = spark.read.
      parquet("src/main/resources/ingestion/data/input/patient_diagnosis.parquet")
    createDeltaTableUsingSparkSQL(spark,patientDiagnosisDf)

    /** Read the ingested delta tables and display them */
    spark.read.format("delta").load("src/main/resources/ingestion/data/output/patients/").show()
    spark.read.format("delta").table("patient_visits").show()
    spark.read.format("delta").table("patient_diagnosis").show()

  }

  /** Spark session needs to be established to get the spark context
   * This initialization is not required when we execute this in Azure Fabric or Databricks
   * As those platforms compute is already initialized and spark context is readily available
   *
   * @return SparkSession
   * */
  def initializeSparkSession(): SparkSession ={
    SparkSession.builder()
      .appName("DataIngestor")
      .master("local[*]")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.sql.warehouse.dir", "src/main/resources/ingestion/data/output/delta_tables/")
      .getOrCreate()
  }

  /**
   * Creates Delta with the input dataframe passed
   * Uses Spark Dataframe write to create delta files to the passes location
   * Mode Overwrite is used during write which truncates existing table and creates new
   *
   * @param patientsDf: Dataframe
   */
  private def createDeltaUsingSparkWrite(patientsDf: DataFrame): Unit = {

    val outputPath = s"src/main/resources/ingestion/data/output/patients"
    patientsDf.write.format("delta")
      .mode("overwrite").saveAsTable("patients")
  }

  /**
   * Creates Delta with the input dataframe passed
   * Uses CTAS(Create Table As Select) to create the delta table
   *
   * @param spark: SparkSession
   * @param patientVisitsDf: Dataframe
   *  */
  private def createDeltaUsingCTAS(spark: SparkSession,patientVisitsDf: DataFrame): Unit = {

    patientVisitsDf.repartition(1).createOrReplaceTempView("visits_view")
    spark.sql(
      """
        |CREATE OR REPLACE TABLE patient_visits
        |USING DELTA
        |AS
        |SELECT visit_id,patient_id,visit_date,department,cost
        |FROM visits_view
      """.stripMargin)

  }

  /**
   * Creates Delta with the input dataframe passed
   * Uses Spark SQL to create the delta table
   *
   * @param spark: SparkSession
   * @param patientDiagnosisDf: Dataframe
   *  */
  private def createDeltaTableUsingSparkSQL(spark: SparkSession,patientDiagnosisDf: DataFrame): Unit={

    patientDiagnosisDf.repartition(1).createOrReplaceTempView("diagnosis_view")
    spark.sql("""DROP TABLE IF EXISTS patient_diagnosis""")

    spark.sql("""
      CREATE TABLE IF NOT EXISTS patient_diagnosis (visit_id INT,
        diagnosis_name STRING
      ) USING DELTA LOCATION 'patient_diagnosis/'
    """)

    spark.sql("""
      INSERT INTO patient_diagnosis SELECT * FROM diagnosis_view
    """)

  }

  /**
   * Creates Delta with the input dataframe passed
   * Uses Spark SQL to create the delta table
   *
   * @param spark: SparkSession
   * @param patientDiagnosisDf: Dataframe
   *  */
  private def createExternalDeltaTableL(spark: SparkSession,patientDiagnosisDf: DataFrame): Unit={

    patientDiagnosisDf.repartition(1).createOrReplaceTempView("diagnosis_view")
    spark.sql("""DROP TABLE IF EXISTS patient_diagnosis""")

    spark.sql(
      """
        |CREATE TABLE patient_visits_external (
        |  visit_id INT,
        |  patient_id INT,
        |  visit_date STRING,
        |  department STRING,
        |  cost DOUBLE
        |)
        |USING delta
        |LOCATION '/data/delta/patient_visits_external'
  """.stripMargin)

  }

}
