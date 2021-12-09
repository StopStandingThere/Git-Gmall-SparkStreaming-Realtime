package com.szl.app

import com.alibaba.fastjson.JSON
import com.szl.bean.UserInfo
import com.szl.constants.GmallConstants
import com.szl.utils.MyKafkaUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

object UserInfoApp {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf
    val sparkConf: SparkConf = new SparkConf().setAppName("UserInfoApp").setMaster("local[*]")

    //2.创建StreamingContext
    val ssc = new StreamingContext(sparkConf,Seconds(3))

    //3.获取kafka数据
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaDStream(GmallConstants.KAFKA_TOPIC_USER,ssc)

    //4.将数据写入Redis中
    kafkaDStream.foreachRDD(rdd=>{
      rdd.foreachPartition(partition=>{
        //获取Redis连接
        val jedis = new Jedis("hadoop102",6379)
        partition.foreach(record=>{
          //获取userInfoRedisKey
          val userInfo: UserInfo = JSON.parseObject(record.value(),classOf[UserInfo])
          val userInfoRedisKey: String = "userInfo:"+userInfo.id

          jedis.set(userInfoRedisKey,record.value())
        })
        jedis.close()
      })
    })

    //将数据转为样例类
    val userInfoDStream: DStream[UserInfo] = kafkaDStream.mapPartitions(partition => {
      partition.map(record => {
        val userInfo: UserInfo = JSON.parseObject(record.value(), classOf[UserInfo])
        userInfo
      })
    })
    userInfoDStream.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
