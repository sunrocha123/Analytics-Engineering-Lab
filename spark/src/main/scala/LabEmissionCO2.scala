import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object LabEmissionCO2 extends App{

  // sessão spark
  val spark = SparkSession.builder()
    .appName("Lab Emission CO2")
    .config("spark.master", "local")
    .getOrCreate()

  // schema

  val schema = StructType(Array(
    StructField("Country", StringType),
    StructField("ISO 3166-1 alpha-3", StringType),
    StructField("Year", LongType),
    StructField("Total", DecimalType(15,2)),
    StructField("Coal", DecimalType(15,2)),
    StructField("Oil", DecimalType(15,2)),
    StructField("Gas", DecimalType(15,2)),
    StructField("Cement", DecimalType(15,2)),
    StructField("Flaring", DecimalType(15,2)),
    StructField("Other", DecimalType(15,2)),
    StructField("Per Capita", DecimalType(15,2))
  ))

  val emissionCO2DF = spark.read
    .format("csv")
    .schema(schema)
    .options(Map(
      "sep" -> ",",
      "header" -> "true"
    ))
    .csv("src/main/resources/data/emissionCO2/GCB2022v27_MtCO2_flat.csv")

  // Remover do DF dados Globais e Internacionais

  val notEqualsGlobal = col("ISO 3166-1 alpha-3").isin("XIT", "WLD")

  val removeDataGlobalInternacional = emissionCO2DF
    .withColumn("notEqualsGlobal", notEqualsGlobal)
    .where(not(col("notEqualsGlobal")))

  // Qtd total e média de CO2 emitido entre 1950 à 1980

  val consumeCO219501980DF = removeDataGlobalInternacional
    .where(col("Year").between(1950, 1980))

  val qtdTotalAvgCO219501980 = consumeCO219501980DF
    .agg(
      sum(col("Total")).as("qtd_total_CO2"),
      avg(col("Total")).as("avg_total_CO2")
    )

  //qtdTotalAvgCO219501980.show()

  // Entre 1950 e 1980, quais foram os TOP 3 países com os maiores índices de emissão de CO2 no mundo

  val topThreeMoreNumberCO2 = consumeCO219501980DF
    .groupBy(col("Country"))
    .agg(
      sum(col("Total")).as("qtd_total_CO2")
    )
    .orderBy(col("qtd_total_CO2").desc)
    .limit(3)

  //topThreeMoreNumberCO2.show()

  // Entre 1950 e 1980, qual foi o ano de maior consumo de CO2 da Russia, USA e Alemanha

  val yearMoreConsumeCO2 = consumeCO219501980DF
    .where(col("Country").isin("USA", "Russia", "Germany"))
    .withColumn("rkConsume_byCountry", row_number()over(Window.partitionBy(col("Country")).orderBy(desc("Total"))))
    .where(col("rkConsume_byCountry") === 1)
    .orderBy(col("Total").desc)

  //yearMoreConsumeCO2.show()

  // Quais são os TOP 5 países com menores índices de emissão de CO2 a partir de 1990

  val topFiveMinorConsumeCO2CountryDF = removeDataGlobalInternacional
    .where(col("Year") >= 1990)
    .groupBy(col("Country"))
    .agg(
      sum(col("Total")).as("qtd_total_CO2"),
      avg(col("Total")).as("avg_total_CO2")
    )
    .orderBy(col("qtd_total_CO2").asc)
    .limit(5)

  //topFiveMinorConsumeCO2CountryDF.show()

  // Diferença de emissão de CO2 entre Russia e USA de 1990 em diante

  val dataRussia = removeDataGlobalInternacional
    .where(col("Year") >= 1990 and col("Country") === "Russia")
    .select(
      col("Year"),
      col("Total").as("consumeRus")
    )

  val dataUSA = removeDataGlobalInternacional
    .where(col("Year") >= 1990 and col("Country") === "USA")
    .select(
      col("Year"),
      col("Total").as("consumeUSA")
    )

  val diferConsumeRusUsa = dataRussia
    .join(dataUSA, "Year")
    .withColumn("Country_consume_more", when(dataRussia.col("consumeRus") > dataUSA.col("consumeUSA"), "RUS")
      .otherwise(lit("USA"))
    )
    .withColumn("difer_consume", when(col("Country_consume_more") === "USA", dataUSA.col("consumeUSA") - dataRussia.col("consumeRus"))
      .otherwise(dataRussia.col("consumeRus") - dataUSA.col("consumeUSA"))
    )
    .select(
      dataUSA.col("Year"),
      col("Country_consume_more"),
      col("difer_consume")
    )

  //diferConsumeRusUsa.show(50)
}
