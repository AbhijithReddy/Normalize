package org.abhijithreddy

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
object NormalizeData {
  def flattenDataFrame(df: DataFrame): DataFrame = {
    val fields = df.schema.fields
    val fieldNames = fields.map(x => x.name)
    val length = fields.length

    for(i <- 0 to fields.length-1){
      val field = fields(i)
      val fieldtype = field.dataType
      val fieldName = field.name
      fieldtype match {
        case arrayType: ArrayType =>
          val fieldNamesExcludingArray = fieldNames.filter(_!=fieldName)
          val fieldNamesAndExplode = fieldNamesExcludingArray ++ Array(s"explode_outer($fieldName) as $fieldName")
          val explodedDf = df.selectExpr(fieldNamesAndExplode:_*)
          return flattenDataFrame(explodedDf)
        case structType: StructType =>
          val childFieldnames = structType.fieldNames.map(childname => fieldName +"."+childname)
          val newfieldNames = fieldNames.filter(_!= fieldName) ++ childFieldnames
          val renamedcols = newfieldNames.map(x => (col(x.toString()).as(x.toString().replace(".", "_"))))
          val explodedf = df.select(renamedcols:_*)
          return flattenDataFrame(explodedf)
        case _ =>
      }
    }
    return df
  }
  def main(args: Array[String]): Unit ={
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("UnifySpark")
      .getOrCreate()
    import spark.implicits._
    spark.sparkContext.setLogLevel("WARN")

    // Load ofac xml data into DataFrame
    val ofac = spark.read
      .format("com.databricks.spark.xml")
      .option("rootTag", "sdnList")
      .option("rowTag", "sdnEntry")
      .load("ofac.xml")

    // Flatten ofac DataFrame
    val ofacFlt = flattenDataFrame(ofac)
    val append_of = ofacFlt.columns.map("of_" + _) // Append prefix 'of' to column_names
    var ofacDF = ofacFlt.toDF(append_of: _*)

    // Transform ofac DataFrame columns to align with uktreasury data
    ofacDF = ofacDF.withColumnRenamed("of_sdnType", "uk_GroupTypeDescription")
    ofacDF = ofacDF.withColumn("of_akaList_aka_type", regexp_replace(col("of_akaList_aka_type"), "\\.", "")).withColumn("of_akaList_aka_type", upper($"of_akaList_aka_type")).withColumnRenamed("of_akaList_aka_type", "uk_AliasType")
    ofacDF = ofacDF.withColumn("of_akaList_aka_category", when(col("of_akaList_aka_category") === "strong", "Good quality").when(col("of_akaList_aka_category") === "weak", "Low quality").otherwise(col("of_akaList_aka_category"))).withColumnRenamed("of_akaList_aka_category", "uk_AliasQuality")
    ofacDF = ofacDF.withColumnRenamed("of_vesselInfo_vesselType", "uk_Ship_Type__VALUE")
    ofacDF = ofacDF.withColumn("uk_Ship_Type__i:nil", when(col("uk_Ship_Type__VALUE").isNull, true).otherwise(false))
    ofacDF = ofacDF.withColumnRenamed("of_vesselInfo_vesselFlag", "uk_Ship_PreviousFlags__VALUE")
    ofacDF = ofacDF.withColumn("uk_Ship_PreviousFlags__i:nil", when(col("uk_Ship_PreviousFlags__VALUE").isNull, true).otherwise(false))
    ofacDF = ofacDF.withColumnRenamed("of_vesselInfo_tonnage", "uk_Ship_Tonnage__VALUE")
    ofacDF = ofacDF.withColumn("uk_Ship_Tonnage__i:nil", when(col("uk_Ship_Tonnage__VALUE").isNull, true).otherwise(false))
    ofacDF = ofacDF.withColumnRenamed("of_dateOfBirthList_dateOfBirthItem_dateOfBirth", "uk_Individual_DateOfBirth")
    ofacDF = ofacDF.withColumnRenamed("of_nationalityList_nationality_country", "uk_Individual_Nationality__VALUE")
    ofacDF = ofacDF.withColumn("uk_Individual_Nationality__i:nil", when(col("uk_Individual_Nationality__VALUE").isNull, true).otherwise(false))
    ofacDF = ofacDF.withColumnRenamed("of_idList_id_idNumber", "uk_Individual_PassportNumber__VALUE")
    ofacDF = ofacDF.withColumn("uk_Individual_PassportNumber__i:nil", when(col("uk_Individual_PassportNumber__VALUE").isNull, true).otherwise(false))
    ofacDF = ofacDF.withColumn("uk_Individual_PassportDetails__VALUE", concat(col("of_idList_id_idCountry"), lit(" number. Issued on "), col("of_idList_id_issueDate"), lit(", expires on "), col("of_idList_id_expirationDate")))
    ofacDF = ofacDF.withColumn("uk_Individual_PassportDetails__i:nil", when(col("uk_Individual_PassportDetails__VALUE").isNull, true).otherwise(false))
    ofacDF = ofacDF.drop("of_idList_id_idCountry", "of_idList_id_issueDate", "of_idList_id_expirationDate")
    ofacDF = ofacDF.withColumnRenamed("of_citizenshipList_citizenship_country", "uk_Country__VALUE")
    ofacDF = ofacDF.withColumn("uk_Country__i:nil", when(col("uk_Country__VALUE").isNull, true).otherwise(false))
    ofacDF = ofacDF.withColumnRenamed("of_title", "uk_Title__VALUE")
    ofacDF = ofacDF.withColumn("uk_Title__i:nil", when(col("uk_Title__VALUE").isNull, true).otherwise(false))
    ofacDF = ofacDF.withColumnRenamed("of_remarks", "uk_OtherInformation__VALUE")
    ofacDF = ofacDF.withColumn("uk_OtherInformation__i:nil", when(col("uk_OtherInformation__VALUE").isNull, true).otherwise(false))
    ofacDF = ofacDF.withColumn("uk_Name6", concat_ws(",", col("of_firstName"), col("of_lastName")))
    ofacDF = ofacDF.drop("of_firstName", "of_lastName")
    // Load uktreasury xml data into DataFrame
    val uktsry = spark.read
      .format("com.databricks.spark.xml")
      .option("rootTag", "ArrayOfFinancialSanctionsTarget")
      .option("rowTag", "FinancialSanctionsTarget")
      .load("uktreasury.xml")

    // Flatten uktreasury DataFrame
    val uktsryFlt = flattenDataFrame(uktsry)
    val prefix_columns = uktsryFlt.columns.map("uk_" + _)
    var uktsryDF = uktsryFlt.toDF(prefix_columns: _*)

    // Transform uktreasury DataFrame columns to align with ofac data
    uktsryDF = uktsryDF.withColumn("uk_Individual_DateOfBirth", date_format(col("uk_Individual_DateOfBirth"),"dd MMM yyyy"))
    uktsryDF = uktsryDF.withColumn("of_placeOfBirthList_placeOfBirthItem_placeOfBirth", concat_ws(", ", col("uk_Individual_TownOfBirth__VALUE"), col("uk_Individual_CountryOfBirth__VALUE")))
    uktsryDF = uktsryDF.drop("uk_Individual_TownOfBirth__VALUE", "uk_Individual_TownOfBirth__i:nil", "uk_Individual_CountryOfBirth__VALUE", "uk_Individual_CountryOfBirth__i:nil")

    // Merge both DataFrames
    val normalized_cols = ofacDF.columns.toSet ++ uktsryDF.columns.toSet
    def getMissingColumns(column: Set[String], normalized_cols: Set[String]) = {
      normalized_cols.toList.map(x => x match {
        case x if column.contains(x) => col(x)
        case _ => lit(null).as(x)
      })
    }
    ofacDF = ofacDF.select(getMissingColumns(ofacDF.columns.toSet, normalized_cols):_*)
    uktsryDF = uktsryDF.select(getMissingColumns(uktsryDF.columns.toSet, normalized_cols):_*)
    uktsryDF = ofacDF.unionByName(uktsryDF).distinct

    uktsryDF.write.parquet("normalizedData.parquet")
    spark.stop()
  }
}
