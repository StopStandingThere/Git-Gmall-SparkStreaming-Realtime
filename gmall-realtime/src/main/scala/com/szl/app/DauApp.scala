package com.szl.app

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.JSON
import com.szl.bean.StartUpLog
import com.szl.constants.GmallConstants
import com.szl.handler.DauHandler
import com.szl.utils.MyKafkaUtil
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.phoenix.spark._

object DauApp {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf
    val sparkConf: SparkConf = new SparkConf().setAppName("DauApp").setMaster("local[*]")

    //2.创建SparkStreamingContext
    val ssc: StreamingContext = new StreamingContext(sparkConf,Seconds(5))

    //3.获取Kafka数据
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaDStream(GmallConstants.KAFKA_TOPIC_STARTUP,ssc)

    //4.将json数据转化为样例类,并补全两个时间字段
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH")
    val startUpLogDStream: DStream[StartUpLog] = kafkaDStream.mapPartitions(partition => {
      partition.map(record => {
        val startUpLog: StartUpLog = JSON.parseObject(record.value(), classOf[StartUpLog])

        val times: String = sdf.format(new Date(startUpLog.ts))
        //补全logDate字段
        startUpLog.logDate = times.split(" ")(0)
        startUpLog.logHour = times.split(" ")(1)
        startUpLog
      })
    })
    //优化:将使用多次的流进行缓存
    startUpLogDStream.cache()

    //5.批次间去重
    val filterByRedisDStream: DStream[StartUpLog] = DauHandler.filterByRedis(startUpLogDStream,ssc.sparkContext)

    //6.批次内去重
    val fliterByGroupDStream: DStream[StartUpLog] = DauHandler.filterByGroup(filterByRedisDStream)

    //7.将去重后的mid存入redis
    DauHandler.saveToRedis(fliterByGroupDStream)

    startUpLogDStream.count().print()
    filterByRedisDStream.count().print()
    fliterByGroupDStream.count().print()

    //8.将去重后的明细数据存入phoenix(Hbase)
    fliterByGroupDStream.foreachRDD(rdd=>{
      rdd.saveToPhoenix(
        "GMALL_SSC_DAU",
        Seq("MID", "UID", "APPID", "AREA", "OS", "CH", "TYPE", "VS", "LOGDATE", "LOGHOUR", "TS"),
        HBaseConfiguration.create,
        Some("hadoop102,hadoop103,hadoop104:2181")
      )
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
