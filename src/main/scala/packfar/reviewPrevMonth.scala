package packfar

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object reviewPrevMonth {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder.master("local[*]").appName(s"OneVsRestExample").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark.conf.set("spark.sql.broadcastTimeout","36000")

    val appleDF = spark.read.format("csv").
      option("header", "true").
      option("inferSchema", "true").
      load("hdfs:///demo/data/aapl-2017.csv").coalesce(1)

         appleDF.show(50)

println(appleDF.javaRDD.getNumPartitions)

    spark.sparkContext.setLogLevel("WARN")
    appleDF.javaRDD.getNumPartitions
    //je céer un clée (just pour implementer l'algorithme)
    val mrcfit_Sans_Previous_kpis = appleDF.withColumn("DATE_ACTION", trunc(col("Date"), "mm")) //je tranc la date (mois)
      .withColumn("ID_STRUCTURE", concat(lit("User"), dayofmonth(col("Date")).cast("String"))) //j'ajoute la clé(id structure on la deja)
      .withColumn("CD_POSTE_TYPE", concat(lit("Name"), dayofyear(col("Date")).cast("String"))) //j'ajoute la clé(id structure on la deja)
      .withColumnRenamed("Volume", "IND_NB_USER_DST")
      .select("DATE_ACTION", "ID_STRUCTURE", "CD_POSTE_TYPE", "IND_NB_USER_DST", "Low", "High")
      ///mes kpis
      .withColumn("var_log_High", abs(log(col("High"))))
      .withColumn("var_exp_High", abs(exp(sin(col("High")))))
      .withColumn("var_cos_High", abs(cos(col("High"))))
      .withColumn("var_sin_High", abs(sin(col("High"))))

      .withColumn("var_log_Low", abs(log(col("Low"))))
      .withColumn("var_exp_Low", abs(exp(sin(col("Low")))))
      .withColumn("var_cos_Low", abs(cos(col("Low"))))
      .withColumn("var_sin_Low", abs(sin(col("Low"))))
      .withColumn("var_sin_Low", abs(sin(col("Low"))))


    def duplic_copute_prev(df: DataFrame, pa: Int): DataFrame = {
      import org.apache.spark.sql.functions._
      import spark.implicits._


      val maxDATE_ACTION = "'" + spark.sql("select max(DATE_ACTION) from vu ").collect().map(u => u(0)).toList.head + "'"

      val df1 = df.withColumn("DATE_ACTION", add_months(col("DATE_ACTION"), pa))
        .filter("DATE_ACTION <= " + maxDATE_ACTION)
        .withColumn("IND_NB_USER_DST", lit(0))
        .withColumn("Low", lit(0))
        .withColumn("High", lit(0))
        .withColumn("var_log_High", lit(0))
        .withColumn("var_exp_High", lit(0))
        .withColumn("var_cos_High", lit(0))
        .withColumn("var_sin_High", lit(0))
        .withColumn("var_log_Low", lit(0))
        .withColumn("var_exp_Low", lit(0))
        .withColumn("var_cos_Low", lit(0))
        .withColumn("var_sin_Low", lit(0))
      val df3 = df.union(df1)
      val df4 = df3.orderBy($"IND_NB_USER_DST".desc).dropDuplicates("DATE_ACTION", "ID_STRUCTURE", "CD_POSTE_TYPE")
      df4
    }

    def duplic_bddf(df: DataFrame): DataFrame = {
      import spark.implicits._
      val dfbddf = duplic_copute_prev(df, 12).
        union(duplic_copute_prev(df, 6)).
        union(duplic_copute_prev(df, 4)).
        union(duplic_copute_prev(df, 3)).
        union(duplic_copute_prev(df, 1))
        .orderBy($"IND_NB_USER_DST".desc).dropDuplicates("DATE_ACTION", "ID_STRUCTURE", "CD_POSTE_TYPE")
      dfbddf
    }

    val joincle = Seq("DATE_ACTION", "ID_STRUCTURE", "CD_POSTE_TYPE")
    val base_kpis = Seq("IND_NB_USER_DST", "Low", "High", "var_log_High", "var_exp_High", "var_cos_High", "var_sin_High",
      "var_log_Low", "var_exp_Low", "var_cos_Low", "var_sin_Low")
    val nomkpis_preced1 = Seq("IND_NB_USER_DST_PM1", "Low_PM1", "High_PM1", "var_log_High_PM1", "var_exp_High_PM1",
      "var_cos_High_PM1", "var_sin_High_PM1", "var_log_Low_PM1", "var_exp_Low_PM1", "var_cos_Low_PM1", "var_sin_Low_PM1")
    val name_all_kpis = joincle
      .union(base_kpis)
      .union(nomkpis_preced1)
      .union(nomkpis_preced1.map(x => x.replace("1", "3")))
      .union(nomkpis_preced1.map(x => x.replace("1", "4")))
      .union(nomkpis_preced1.map(x => x.replace("1", "6")))
      .union(nomkpis_preced1.map(x => x.replace("1", "12")))

    //je dupliquqe
    val v0 = duplic_bddf(mrcfit_Sans_Previous_kpis)
    //j'incremente les date sur des different dataframe
    val v1 = v0.withColumn("DATE_ACTION", add_months(col("DATE_ACTION"), 1))
    val v3 = v0.withColumn("DATE_ACTION", add_months(col("DATE_ACTION"), 3))
    val v4 = v0.withColumn("DATE_ACTION", add_months(col("DATE_ACTION"), 4))
    val v6 = v0.withColumn("DATE_ACTION", add_months(col("DATE_ACTION"), 6))
    val v12 = v0.withColumn("DATE_ACTION", add_months(col("DATE_ACTION"), 12))
    //jointure des kpis
    val vf = v0.join(v1, joincle, joinType = "left")
      .join(v3, joincle, joinType = "left")
      .join(v4, joincle, joinType = "left")
      .join(v6, joincle, joinType = "left")
      .join(v12, joincle, joinType = "left").toDF(name_all_kpis: _*).na.fill(0)

    val test2 = vf.select("DATE_ACTION", "IND_NB_USER_DST", "IND_NB_USER_DST_PM1", "IND_NB_USER_DST_PM3",
      "IND_NB_USER_DST_PM4", "IND_NB_USER_DST_PM6", "IND_NB_USER_DST_PM12")
      .groupBy("DATE_ACTION")
      .sum()

            test2
              .orderBy("DATE_ACTION")
              .coalesce(1)
              .write
              .format("csv")
             .save("hdfs:///demo/data/test")

  }
}