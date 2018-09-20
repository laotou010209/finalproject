package com.laotou

import java.util.Properties
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object HotCount {
  val conf = new SparkConf().setAppName("HotCount").setMaster("local[*]")
  val sc = new SparkContext(conf)
  //创建hive实例
  val hiveContext = new HiveContext(sc)

  def main(args: Array[String]): Unit = {
    locadMysqlTable()
    sparkBaseData()
    saprkAreaData()
    //TOP K的处理  TOP 3 使用hive开窗函数
    hiveContext.sql("select *,row_number() over(partition by areaName order by clickNum desc) rnb from spark_area_data").registerTempTable("spark_row_number")
    hiveContext.sql("select areaLevelName,areaName,productName,clickNum,extendInfo from " +
      "spark_row_number where rnb<=3 order by areaLevelName asc,areaName " +
      "desc").write.format("jdbc").mode(SaveMode.Append).jdbc("jdbc:mysql://192.168.100.121/big_data?user=root&password=1234","t_result",new Properties())

  }

  /**
    * 加载mysql的表
    */
  def locadMysqlTable(): Unit = {
    //1、	在mysql数据库当中准备相应的表与数据
    //2、spark sql去连接mysql并将mysql当中的表加载到spark sql当中
    var jdbcArea = Map("url" -> "jdbc:mysql://192.168.100.121/big_data?user=root&password=1234", "dbtable" -> "t_area")
    var jdbcProduct= Map("url" -> "jdbc:mysql://192.168.100.121/big_data?user=root&password=1234", "dbtable" -> "t_product")
    val reader = hiveContext.read.format("jdbc")
    //将mysql的表注册成spark的表
    reader.options(jdbcArea).load().registerTempTable("spark_area")
    reader.options(jdbcProduct).load().registerTempTable("spark_product")
  }

  /**
    * 处理关联数据
    */
  def sparkBaseData():Unit={
    hiveContext.sql("use yangfengbing")
    //3、进行关联查询,让mysq加载出来的数据与hive当中的表进行关联，并产生一个子表的结果
    hiveContext.sql("select spark_area.area_Name areaName, " +
      "spark_product.product_name productName,count(1) clickNum,spark_product.extend_info extendInfo " +
      "from t_click join spark_area " +
      "on t_click.city_id=spark_area.city_id " +
      "join spark_product on " +
      "t_click.click_product_id=spark_product.product_id " +
      "group by spark_area.area_Name,spark_product.product_name,spark_product.extend_info").registerTempTable("spark_base_data")
  }

  /**
    * 处理地区数据
    */
  def saprkAreaData():Unit={
    hiveContext.sql("select CASE " +
      "WHEN areaName in('华北地区','东北地区') THEN 'A' " +
      "WHEN areaName in('华东地区','华中地区') THEN 'B' " +
      "WHEN areaName in('华南地区','西南地区') THEN 'C' " +
      "else 'D' end areaLevelName,areaName,productName,clickNum,if(extendInfo=1,'自营','第三方') " +
      "extendInfo from spark_base_data").registerTempTable("spark_area_data")
  }
}