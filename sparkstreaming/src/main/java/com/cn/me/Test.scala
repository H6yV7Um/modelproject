package com.cn.me

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Milliseconds, Seconds, StreamingContext}

object Test {
  def main(args: Array[String]) {
    //创建一个本地的StreamingContext，含2个工作线程
    val conf = new SparkConf().setMaster("local[2]").setAppName("ScoketStreaming")
    val sc = new StreamingContext(conf,Seconds(10))   //每隔10秒统计一次字符总数
    //创建珍一个DStream，连接master:9998
    val lines = sc.socketTextStream("localhost",9998)
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x , 1)).reduceByKey(_ + _)
    wordCounts.print()
    sc.start()         //开始计算
    sc.awaitTermination()   //通过手动终止计算，否则一直运行下去
  }
    //    val conf = new SparkConf()
//      .setMaster("local[2]")
//      .setAppName("LocalTest")
//    // WARN StreamingContext: spark.master should be set as local[n], n > 1 in local mode if you have receivers to get data,
//    // otherwise Spark jobs will not get resources to process the received data.
//    val sc = new StreamingContext(conf, Milliseconds(5000))
//    sc.checkpoint("flumeCheckpoint/")
//    val messages = ssc.socketTextStream("localhost", 9998)

}
