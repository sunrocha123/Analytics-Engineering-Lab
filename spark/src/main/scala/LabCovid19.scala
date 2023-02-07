import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object LabCovid19 extends App{

  // sessão spark
  val spark = SparkSession.builder()
    .appName("Lab COVID-19")
    .config("spark.master", "local")
    .getOrCreate()

  // schema
  val schema = StructType(Array(
    StructField("iso_code", StringType),
    StructField("continent", StringType),
    StructField("location", StringType),
    StructField("date", DateType),
    StructField("total_cases", DecimalType(30,2)),
    StructField("new_cases", DecimalType(30,2)),
    StructField("new_cases_smoothed", DecimalType(30,2)),
    StructField("total_deaths", DecimalType(30,2)),
    StructField("new_deaths", DecimalType(30,2)),
    StructField("new_deaths_smoothed", DecimalType(30,2)),
    StructField("total_cases_per_million", DecimalType(30,2)),
    StructField("new_cases_per_million", DecimalType(30,2)),
    StructField("new_cases_smoothed_per_million", DecimalType(30,2)),
    StructField("total_deaths_per_million", DecimalType(30,2)),
    StructField("new_deaths_per_million", DecimalType(30,2)),
    StructField("new_deaths_smoothed_per_million", DecimalType(30,2)),
    StructField("reproduction_rate", DecimalType(30,2)),
    StructField("icu_patients", DecimalType(30,2)),
    StructField("icu_patients_per_million", DecimalType(30,2)),
    StructField("hosp_patients", DecimalType(30,2)),
    StructField("hosp_patients_per_million", DecimalType(30,2)),
    StructField("weekly_icu_admissions", DecimalType(30,2)),
    StructField("weekly_icu_admissions_per_million", DecimalType(30,2)),
    StructField("weekly_hosp_admissions", DecimalType(30,2)),
    StructField("weekly_hosp_admissions_per_million", DecimalType(30,2)),
    StructField("new_tests", DecimalType(30,2)),
    StructField("total_tests", DecimalType(30,2)),
    StructField("total_tests_per_thousand", DecimalType(30,2)),
    StructField("new_tests_per_thousand", DecimalType(30,2)),
    StructField("new_tests_smoothed", DecimalType(30,2)),
    StructField("new_tests_smoothed_per_thousand", DecimalType(30,2)),
    StructField("positive_rate", DecimalType(30,2)),
    StructField("tests_per_case", DecimalType(30,2)),
    StructField("tests_units", StringType),
    StructField("total_vaccinations", DecimalType(30,2)),
    StructField("people_vaccinated", DecimalType(30,2)),
    StructField("people_fully_vaccinated", DecimalType(30,2)),
    StructField("total_boosters", DecimalType(30,2)),
    StructField("new_vaccinations", DecimalType(30,2)),
    StructField("new_vaccinations_smoothed", DecimalType(30,2)),
    StructField("total_vaccinations_per_hundred", DecimalType(30,2)),
    StructField("people_vaccinated_per_hundred", DecimalType(30,2)),
    StructField("people_fully_vaccinated_per_hundred", DecimalType(30,2)),
    StructField("total_boosters_per_hundred", DecimalType(30,2)),
    StructField("new_vaccinations_smoothed_per_million", DecimalType(30,2)),
    StructField("new_people_vaccinated_smoothed", DecimalType(30,2)),
    StructField("new_people_vaccinated_smoothed_per_hundred", DecimalType(30,2)),
    StructField("stringency_index", DecimalType(30,2)),
    StructField("population", DecimalType(30,2)),
    StructField("population_density", DecimalType(30,2)),
    StructField("median_age", DecimalType(30,2)),
    StructField("aged_65_older", DecimalType(30,2)),
    StructField("aged_70_older", DecimalType(30,2)),
    StructField("gdp_per_capita", DecimalType(30,2)),
    StructField("extreme_poverty", DecimalType(30,2)),
    StructField("cardiovasc_death_rate", DecimalType(30,2)),
    StructField("diabetes_prevalence", DecimalType(30,2)),
    StructField("female_smokers", DecimalType(30,2)),
    StructField("male_smokers", DecimalType(30,2)),
    StructField("handwashing_facilities", DecimalType(30,2)),
    StructField("hospital_beds_per_thousand", DecimalType(30,2)),
    StructField("life_expectancy", DecimalType(30,2)),
    StructField("human_development_index", DecimalType(30,2)),
    StructField("excess_mortality_cumulative_absolute", DecimalType(30,2)),
    StructField("excess_mortality_cumulative", DecimalType(30,2)),
    StructField("excess_mortality", DecimalType(30,2)),
    StructField("excess_mortality_cumulative_per_million", DecimalType(30,2))
  ))

  val covidDF = spark.read
    .options(Map(
      "header" -> "true",
      "sep" -> ","
    ))
    .schema(schema)
    .csv("src/main/resources/data/owid-covid-data.csv")
    .where(col("continent").isNotNull)

  //covidDF.show()

  // qtd de países por continente que estão no dataset

  val qtdCountriesByContinent = covidDF
    .select(
      col("continent"),
      col("location")
    )
    .distinct()
    .groupBy(col("continent"))
    .count()
    .orderBy(desc("count"))

  //qtdCountriesByContinent.show()

  // Quantidade de mortes por ano

  val qtdDeathByYearAndContinent = covidDF
    .groupBy(year(col("date")))
    .agg(
      sum(col("new_deaths")).as("qtd_deaths")
    )
    .orderBy(desc("qtd_deaths"))

  //qtdDeathByYearAndContinent.show()

  // TOP 2 continentes com maiores números de mortes e casos em 2022

  val topTwoContinentMoreDeathCases2022 = covidDF
    .where(year(col("date")) === 2022)
    .groupBy(col("continent"))
    .agg(
      sum(col("new_cases")).as("qtd_cases"),
      sum(col("new_deaths")).as("qtd_deaths")
    )
    .orderBy(col("qtd_cases").desc, col("qtd_deaths").desc)
    .limit(2)

  //topTwoContinentMoreDeathCases2022.show()

  // TOP 5 países que tiveram mais casos no ano de 2021

  val topFiveCountriesMoreNewCases2020 = covidDF
    .where(year(col("date")) === 2021)
    .groupBy(col("location"))
    .agg(
      sum(col("new_cases")).as("qtd_cases")
    )
    .orderBy(desc("qtd_cases"))
    .limit(5)

  //topFiveCountriesMoreNewCases2020.show()

  //covidDF.where(col("people_vaccinated").isNotNull).show()

  // TOP 3 países que tiveram o maior número de mortes em 2021

  val topThreeCountriesMoreNewDeaths2021 = covidDF
    .where(year(col("date")) === 2021)
    .groupBy(col("location"))
    .agg(
      sum(col("new_deaths")).as("qtd_deaths")
    )
    .orderBy(desc("qtd_deaths"))
    .limit(3)

  //topThreeCountriesMoreNewDeaths2021.show()

  // Comparativo de mortes no 1º Semestre de 2020 X 2021 no Brasil

  val CompDeathsSemesterInBrazil = covidDF
    .where(col("location") === "Brazil")
    .where(col("date").between("2020-01-01", "2020-06-30") or
      col("date").between("2021-01-01", "2021-06-30")
    )
    .withColumn("semester", when(col("date").between("2020-01-01", "2020-06-30"), "1/2020")
      .otherwise(lit("1/2021"))
    )
    .groupBy(col("semester"))
    .agg(
      sum(col("new_deaths")).as("sum_deaths"),
      avg(col("new_deaths")).as("avg_deaths")
    )
    .orderBy(col("semester").desc)

  //CompDeathsSemesterInBrazil.show()

}
