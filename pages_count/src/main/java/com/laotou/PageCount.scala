package com.laotou

import java.math.{BigDecimal, RoundingMode}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object PageCount {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("PageCount")
    val sc: SparkContext = new SparkContext(conf)
    //拿到格式化之后的数据
    //   (sessionId,(pageId,actionTime))
    val formateData: RDD[(String, (String, String))] = getInitDataFormate(sc).persist(StorageLevel.MEMORY_AND_DISK_2)
    //根据sessionid将一个用户一次访问的数据组合在一起。
    val groupByRdd: RDD[(String, Iterable[(String, String)])] = formateData.groupByKey()
    val formateResultRDD: RDD[String] = getSplitPages(groupByRdd)
    //从数据库中拿出用户需要查询的页面流
    val dbArray: ArrayBuffer[String] = getDbData()
    //开始匹配,拿到用户访问流中的数据(6_9,2)(1_3,1)(3_6,2)
    val stringToLong: collection.Map[String, Long] = pipeiData(formateResultRDD,dbArray)
    //去拿到页面1的访问次数
    val firstPageId = dbArray(0).split("_")(0)
    //统计出firstPageId一共有多少个点击量
   var withString=  firstPageId+"_"
    //得到首页点击量
    val firstPageNum = formateResultRDD.filter(_.startsWith(withString)).count()
//    println(firstPageNum)
    //计算
    divResult(firstPageNum,dbArray,stringToLong).foreach(println(_))

  }

  /**
    * 求出最后结果进行计算
    * @param firstPageNum
    * @param dbArray
    * @param stringToLong
    */
  def divResult(firstPageNum: Long, dbArray: ArrayBuffer[String], stringToLong: collection.Map[String, Long])={
    //将循环之后的结果放到result中
    var result=new mutable.HashMap[String,Double]()
    //如果要保证顺序可以采用Array[(String,Dobule)]
    var upresult:Long=0L
    for(index <-0 until dbArray.length){
      val maybeLong = stringToLong.get(dbArray(index)).get
      //表示需要用到firstPageNum
      //能从合并之后的数据中拿出当前key的点击次数
      var resultDouble=0.0D
      if(index ==0){
        //先取出1_3的总数量
        resultDouble=div(maybeLong,firstPageNum,4)
      }else{
        resultDouble=div(maybeLong,upresult,4)
      }
      result+=(dbArray(index)->resultDouble)
      upresult=maybeLong
    }
    result
  }
  /**
    * 计算的工具类  进行4舍5入的处理
    * @param v1
    * @param v2
    * @param scale  保留多少位小数
    * @return
    */
  def div(v1:Double,v2:Double,scale:Int) ={
    var b1=new BigDecimal(v1)
    var b2=new BigDecimal(v2)
    b1.divide(b2, scale, RoundingMode.HALF_UP).doubleValue()*100
  }
  /**
    * 从用户的访问流当中，找出符合要求的数据
    * @param formateResultRDD
    * @param dbArray
    */
  def pipeiData(formateResultRDD: RDD[String],dbArray: ArrayBuffer[String]): collection.Map[String, Long] ={
    //formateResultRDD.filter(dbArray.contains(_)).map((_,1)).reduceByKey(_+_).foreach(println(_))
    formateResultRDD.filter(dbArray.contains(_)).map((_,1)).countByKey()
  }

  /**
    * 拿到数据库中的数据，并将数据格式转换成用户想要的格式
    */
  def  getDbData() ={
  var data="1,3,6,9"
    var dbArray=new ArrayBuffer[String]()
    data.split(",").reduceLeft((value1,value2)=>{
      dbArray+=value1+"_"+value2
      value2
    })
    dbArray
}
  /**
    *
    * 将用户点击的页面流，处理成一个一个的关联数据
    * @param groupByRdd
    */
  def getSplitPages(groupByRdd: RDD[(String, Iterable[(String, String)])])={
  //将一个用户的数据进行排序
//    groupByRdd.map(data=>{
//      val value: Iterable[(String, String)] = data._2
//      val tuples: Array[String] = value.toArray.sortBy(_._2).map(_._1)
////      tuples.foreach(println(_)) //先转为数组，获取sessionId的访问流
//
//      for(index <- 1 until(tuples.length)){
//        val upPageId = tuples(index-1)
//        //得到的是上一个页面的pageid
//        val currentPageId= tuples(index)
//        val str = upPageId+"_"+currentPageId
//        //得到当前pageId
//        println(str)
//      }
//    }).collect()

    //----------------------------
    groupByRdd.flatMap(data=>{
      var reduceCountArray=new ArrayBuffer[String]()
      val tuples= data._2.toArray.sortBy(_._2).map(_._1).reduceLeft((value1,value2)=>{
        reduceCountArray+= value1+"_"+value2
        value2
      })
      reduceCountArray
    })

  }
  /**
    * 拿到文件内容，并将数据进行格式化
    * @param sc
    */
  def getInitDataFormate(sc: SparkContext)={
    sc.textFile("d:/big_data.txt").map(data=>{
      //需要判断data是否为空
      val dataArrys = data.split("\t")
      //在返回值的时候，将数组长度判断一下
      (dataArrys(2),(dataArrys(3),dataArrys(4)))
    })
  }
}
