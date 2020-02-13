import java.{lang, util}

import com.alibaba.fastjson.JSON
import com.niuzhengyu.gmall0826.common.constant.GmallConstant
import com.niuzhengyu.gmall0826.realtime.bean.OrderInfo
import com.niuzhengyu.gmall0826.realtime.util.{MyKafkaUtil, RedisUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.phoenix.spark._
import redis.clients.jedis.Jedis

object OrderApp {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("order_app").setMaster("local[*]")
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(5))

    val inputDstream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstant.KAFKA_TOPIC_ORDER, ssc)

    val orderInfoDstream: DStream[OrderInfo] = inputDstream.map {
      record => {
        val jsonString: String = record.value()
        val orderInfo: OrderInfo = JSON.parseObject(jsonString, classOf[OrderInfo])

        // 补充 日期字段
        val datetimeArr: Array[String] = orderInfo.create_time.split(" ")
        orderInfo.create_date = datetimeArr(0)
        orderInfo.create_hour = datetimeArr(1).split(":")(0)

        // 获取电话
        val teltuple: (String, String) = orderInfo.consignee_tel.splitAt(3)
        // 数据脱敏 不要加密 直接打乱就行  111****8888
        orderInfo.consignee_tel = teltuple._1 + "****" + teltuple._2.splitAt(4)._2
        //  每个订单 增加一个字段 标识是否是该用户首次付费  is_first_order ( 1 ,0)
        //  要知道 该用户之前是否是消费用户  //如果有个表记录 所有已存在的消费用户清单（redis , mysql  )
        //  作业 尝试用redis保存维护消费用户清单，  在利用清单更新 is_first_order 该字段

        val jedis: Jedis = RedisUtil.getJedisClient
        val key = "user_id"
        val firstOrderId: util.Set[String] = jedis.smembers(key)
        if (jedis.sismember(key, orderInfo.user_id)) {
          orderInfo.is_first_order = "0"
        } else {
          orderInfo.is_first_order = "1"
        }
        jedis.sadd(key, orderInfo.user_id)
        jedis.close()
        orderInfo
      }
    }
    orderInfoDstream.foreachRDD {
      rdd => {
        rdd.saveToPhoenix("GMALL0826_ORDER_INFO",
          Seq("ID","PROVINCE_ID", "CONSIGNEE", "ORDER_COMMENT", "CONSIGNEE_TEL", "ORDER_STATUS", "PAYMENT_WAY", "USER_ID","IMG_URL", "TOTAL_AMOUNT", "EXPIRE_TIME", "DELIVERY_ADDRESS", "CREATE_TIME","OPERATE_TIME","TRACKING_NO","PARENT_ORDER_ID","OUT_TRADE_NO", "TRADE_BODY", "CREATE_DATE", "CREATE_HOUR", "IS_FIRST_ORDER"),
          new Configuration, Some("hadoop102,hadoop103,hadoop104:2181"))
      }
    }
    ssc.start()
    ssc.awaitTermination()
  }
}
