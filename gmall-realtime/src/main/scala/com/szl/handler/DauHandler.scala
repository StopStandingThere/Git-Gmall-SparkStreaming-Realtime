package com.szl.handler

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.szl.bean.StartUpLog
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

object DauHandler {
  /**
    * 将去重后的mid保存到redis
    * @param fliterByGroupDStream
    */
  def saveToRedis(fliterByGroupDStream: DStream[StartUpLog]) = {
    fliterByGroupDStream.foreachRDD(rdd=>{
      rdd.foreachPartition(partition=>{
        //在每个分区下创建redis连接
        val jedis = new Jedis("hadoop102",6379)
        partition.foreach(log=>{
          jedis.sadd("DAU:"+log.logDate,log.mid)
        })
        jedis.close()
      })
    })
  }

  /**
    * 批次内去重
    *
    * @param filterByRedisDStream
    */
  def filterByGroup(filterByRedisDStream: DStream[StartUpLog]) ={
    //先将数据转换为k,v结构数据
    val midWithDateToLogDStream: DStream[((String, String), StartUpLog)] = filterByRedisDStream.map(log => {
      ((log.mid, log.logDate), log)
    })
    //将相同key的数据聚合
    val midWithDateToIterLogDStream: DStream[((String, String), Iterable[StartUpLog])] = midWithDateToLogDStream.groupByKey()
    //对相同key的数据进行排序
    val midWithDateToListLogDStream: DStream[((String, String), List[StartUpLog])] = midWithDateToIterLogDStream.mapValues(iter => {
      iter.toList.sortWith(_.ts < _.ts).take(1)
    })
    //获取list集合中的每一个元素
    val value: DStream[StartUpLog] = midWithDateToListLogDStream.flatMap(_._2)
    value
  }

  /**
    * 批次间去重
    * @param startUpLogDStream
    * @param sc
    * @return
    */
  def filterByRedis(startUpLogDStream: DStream[StartUpLog], sc: SparkContext)  = {
    //方案:在每个分区下获取连接,来优化连接数
    /*    val value: DStream[StartUpLog] = startUpLogDStream.mapPartitions(partition => {
          //获取redis连接
          val jedis = new Jedis("hadoop102", 6379)
          val logs: Iterator[StartUpLog] = partition.filter(log => {
            //获取reidsKey
            val redisKey: String = "DAU:" + log.logDate
            val bool: Boolean = jedis.sismember(redisKey, log.mid)
            !bool
          })
          jedis.close()
          logs
        })
        value*/

    //方案:在每个批次内获取一次连接
    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val value: DStream[StartUpLog] = startUpLogDStream.transform(rdd => {
      //1.一个批次内执行一次,在Driver端执行
      val jedis = new Jedis("hadoop102", 6379)

      //2.获取redis中的数据
      val redisKey: String = "DAU:" + sdf.format(new Date(System.currentTimeMillis()))
      val mids: util.Set[String] = jedis.smembers(redisKey)

      //3.将Driver端查出的mids广播至Executor端进行去重
      val midBC: Broadcast[util.Set[String]] = sc.broadcast(mids)

      //4.去重
      val filterMidRDD: RDD[StartUpLog] = rdd.filter(log => {
        val bool: Boolean = midBC.value.contains(log.mid)
        !bool
      })
      filterMidRDD
    })
    value

  }

}
