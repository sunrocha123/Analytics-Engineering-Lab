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

  // TOP 5 países que tiveram mais novos casos no ano de 2020

  val topFiveCountriesMoreNewCases2020 = covidDF
    .where(year(col("date")) === 2020)
    .groupBy(col("location"))
    .agg(
      sum(col("new_cases")).as("qtd_new_cases")
    )
    .orderBy(desc("qtd_new_cases"))
    .limit(5)

  //topFiveCountriesMoreNewCases2020.show()

}
