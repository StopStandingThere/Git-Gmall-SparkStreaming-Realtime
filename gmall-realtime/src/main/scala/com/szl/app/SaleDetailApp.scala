package com.szl.app

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.alibaba.fastjson.JSON
import com.szl.bean.{OrderDetail, OrderInfo, SaleDetail, UserInfo}
import com.szl.constants.GmallConstants
import com.szl.utils.{MyEsUtil, MyKafkaUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis
import org.json4s.native.Serialization

import collection.JavaConverters._

object SaleDetailApp {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf
    val sparkConf: SparkConf = new SparkConf().setAppName("SaleDetailApp").setMaster("local[*]")

    //2.创建StreamingContext
    val ssc: StreamingContext = new StreamingContext(sparkConf,Seconds(3))

    //3.消费Kafka数据
    //获取订单表数据
    val orderInfoKafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaDStream(GmallConstants.KAFKA_TOPIC_ORDER,ssc)

    //获取订单明细表数据
    val orderDetailKafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaDStream(GmallConstants.KAFKA_TOPIC_ORDER_DETAIL,ssc)

    //4.分别将两张表的数据转为样例类
    //订单表
    val orderInfoDStream: DStream[(String, OrderInfo)] = orderInfoKafkaDStream.mapPartitions(partition => {
      partition.map(record => {
        val orderInfo: OrderInfo = JSON.parseObject(record.value(), classOf[OrderInfo])

        //补全时间字段
        orderInfo.create_date = orderInfo.create_time.split(" ")(0)
        orderInfo.create_hour = orderInfo.create_time.split(" ")(1).split(":")(0)

        //双流join需要连接键id,所以返回一个kv类型
        (orderInfo.id, orderInfo)
      })
    })

    //订单明细表
    val orderDetailDStream: DStream[(String, OrderDetail)] = orderDetailKafkaDStream.mapPartitions(partition => {
      partition.map(record => {
        val orderDetail: OrderDetail = JSON.parseObject(record.value(), classOf[OrderDetail])

        (orderDetail.order_id, orderDetail)
      })
    })
    
    //5.对两条流进行join
    val fullJoinDStream: DStream[(String, (Option[OrderInfo], Option[OrderDetail]))] = orderInfoDStream.fullOuterJoin(orderDetailDStream)

    //6.利用缓存的方式来处理因网络延迟所带来的数据丢失问题
    val noUserSaleDetailDStream: DStream[SaleDetail] = fullJoinDStream.mapPartitions(partition => {

      implicit val formats = org.json4s.DefaultFormats

      //创建存放结果的集合
      val details: util.ArrayList[SaleDetail] = new util.ArrayList[SaleDetail]()

      //创建Redis连接
      val jedis = new Jedis("hadoop102", 6379)

      partition.foreach {
        case (orderId, (infoOpt, detailOpt)) =>

          //orderInfoRedisKey
          val orderInfoRedisKey: String = "orderInfo:" + orderId

          //orderDetailRedisKey
          val orderDetailRedisKey: String = "orderDetail:" + orderId

          //TODO a.判断orderInfo数据是否存在
          if (infoOpt.isDefined) {
            //数据存在
            val orderInfo: OrderInfo = infoOpt.get

            //TODO a.2 判断orderDetail是否存在
            if (detailOpt.isDefined) {
              //数据存在
              val orderDetail: OrderDetail = detailOpt.get

              val detail = new SaleDetail(orderInfo, orderDetail)

              //将关联后的样例类存入结果集合
              details.add(detail)
            }
            //TODO b.将orderInfo数据写入缓存Redis
            val orderInfoJson: String = Serialization.write(orderInfo)
            jedis.set(orderInfoRedisKey, orderInfoJson)

            //对数据设置一个过期时间防止内存不够
            jedis.expire(orderInfoRedisKey, 20)

            //TODO c.查询对方缓存中是否有orderDetail数据
            //c.2判断RedisKey是否存在
            if (jedis.exists(orderDetailRedisKey)) {
              //取出orderDetail数据
              val detailsJSONstr: util.Set[String] = jedis.smembers(orderDetailRedisKey)
              //将Java集合转为Scala集合
              for (elem <- detailsJSONstr.asScala) {
                //将查询出来的json字符串的orderDetail数据转为样例类
                val orderDetail: OrderDetail = JSON.parseObject(elem, classOf[OrderDetail])
                val detail = new SaleDetail(orderInfo, orderDetail)
                details.add(detail)
              }
            }
          } else {
            //orderInfo数据不存在
            //TODO d.判断orderDetail是否存在
            if (detailOpt.isDefined) {
              val orderDetail: OrderDetail = detailOpt.get

              //d.2去对方缓存中查询是否有对应的orderInfo数据
              //判断orderInfo的RedisKey是否存在
              if (jedis.exists(orderInfoRedisKey)) {
                //取出orderInfo数据
                val infoJSONStr: String = jedis.get(orderInfoRedisKey)
                //将查询出的字符串转为样例类
                val orderInfo: OrderInfo = JSON.parseObject(infoJSONStr, classOf[OrderInfo])
                //  组合成SaleDetail样例类
                val detail = new SaleDetail(orderInfo, orderDetail)
                //将SaleDetail写入结果集合
                details.add(detail)
              } else {
                //TODO e.对方缓存中没有有对应orderInfo数据,将orderDetail数据存入缓存Redis
                //将orderDetail数据转为JSON字符串
                val detailJSONStr: String = Serialization.write(orderDetail)
                jedis.sadd(orderDetailRedisKey, detailJSONStr)
                //对数据设置过期时间,防止内存不够
                jedis.expire(orderDetailRedisKey, 20)
              }
            }
          }
      }
      //在每个分区下关闭连接
      jedis.close()
      //将结果集转为Scala集合并转为迭代器返回
      details.asScala.toIterator
    })

    //打印测试双流join+缓存的方式是否解决了因网络延迟所带来的数据丢失问题
    //noUserSaleDetailDStream.print()

    //7.反查缓存关联userInfo数据
    val saleDetailDStream: DStream[SaleDetail] = noUserSaleDetailDStream.mapPartitions(partition => {
      //获取Redis连接
      val jedis = new Jedis("hadoop102", 6379)
      val details: Iterator[SaleDetail] = partition.map(saleDetail => {
        //获取用户表数据
        //userInfoRedisKey
        val userInfoRedisKey: String = "userInfo:" + saleDetail.user_id
        val userInfoJsonStr: String = jedis.get(userInfoRedisKey)

        //将userInfo数据转为样例类
        val userInfo: UserInfo = JSON.parseObject(userInfoJsonStr, classOf[UserInfo])

        //关联userInfo
        saleDetail.mergeUserInfo(userInfo)

        saleDetail
      })
      jedis.close()
      details
    })
    saleDetailDStream.print()

    //8.将关联后的明细数据写入ES
    saleDetailDStream.foreachRDD(rdd=>{
      rdd.foreachPartition(partition=>{
        val list: List[(String, SaleDetail)] = partition.toList.map(saleDetail => {
          //将订单明细表的ID作为ES的docid,因为它不会重复,粒度更小
          (saleDetail.order_detail_id, saleDetail)
        })

        val dateStr: String = new SimpleDateFormat("yyyy-MM-dd").format(new Date())
        MyEsUtil.insertBulk(GmallConstants.ES_DETAIL_INDEXNAME+"_"+dateStr,list)
      })
    })
    //开启任务并阻塞
    ssc.start()
    ssc.awaitTermination()
  }
}
