package com.niuzhengyu.gmall0826.realtime.app

import java.{lang, util}
import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.JSON
import com.niuzhengyu.gmall0826.common.constant.GmallConstant
import com.niuzhengyu.gmall0826.realtime.bean.StartUpLog
import com.niuzhengyu.gmall0826.realtime.util.{MyKafkaUtil, RedisUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis
import org.apache.phoenix.spark._


object DauApp {

  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val sparkconf: SparkConf = new SparkConf().setAppName("dau_app").setMaster("local[*]")

    //2.创建上下文
    val ssc: StreamingContext = new StreamingContext(sparkconf, Seconds(5))

    // 获取消费Kafka的流
    val recordDstream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstant.KAFKA_TOPIC_STARTUP, ssc)

    //recordDstream.map(_.value()).print()

    // 把采集到的数据进行格式的转换，补充时间字段
    val startUpLogDstream: DStream[StartUpLog] = recordDstream.map {
      record =>
        // 获取record的value
        val jsonString: String = record.value()
        // 将json串转化为样例类
        val startUpLog: StartUpLog = JSON.parseObject(jsonString, classOf[StartUpLog])
        // 将时间戳转化为标准时间
        val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH")
        // new Date(startUpLog.ts) 将时间戳转化为时间对象
        // 再将时间对象按照以上格式转化为时间字符串
        val datetimeString: String = dateFormat.format(new Date(startUpLog.ts))
        // 将时间字符串切割
        val datetimeArr: Array[String] = datetimeString.split(" ")
        // 将字段补全
        startUpLog.logDate = datetimeArr(0)
        startUpLog.logHour = datetimeArr(1)

        // 再将样例类返回
        startUpLog
    }

    // 去重 保留每个mid当日的第一条，其他的启动日志过滤掉
    // 然后再利用清单进行过滤筛选，把清单中已有的用户的新日志过滤掉

   /* 方案A  连接redis次数过多，可以进一步优化
     startUpLogDstream.filter {
      startuplog => {
        val jedis: Jedis = RedisUtil.getJedisClient
        val daukey: String = "dau:" + startuplog.logDate
        // 通过key看mid是否存在
        val flag: lang.Boolean = !jedis.sismember(daukey, startuplog.mid)
        jedis.close()
        flag
      }
    }*/

    // 优化：利用driver查询出完成的清单，然后利用广播变量发送给每个executor
    // 各个executor  利用广播变量中的清单，检查自己的数据是否需要过滤，在清单中的一律清洗掉
    // 1、查driver
    /* 方案B  错误代码  会造成driver只执行了一次，不会周期执行
    val jedis: Jedis = RedisUtil.getJedisClient
    val today: String = new SimpleDateFormat("yyyy-MM-dd").format(new Date())
    val daukey: String = "dau:" + today
    val midSet: util.Set[String] = jedis.smembers(daukey)
    jedis.close()
    // 2、发
    val midBC: Broadcast[util.Set[String]] = ssc.sparkContext.broadcast(midSet)
    // 3、executor收  筛查
    startUpLogDstream.filter {
      startuplog => {
        val midSet: util.Set[String] = midBC.value
        !midSet.contains(startuplog.mid)
      }
    }*/

    // 方案C
    // 让driver每5秒执行一次
    val filteredDstream: DStream[StartUpLog] = startUpLogDstream.transform {
      rdd => {
        // driver每5秒执行一次
        // 1、查driver
        println("过滤前：" + rdd.count())
        val jedis: Jedis = RedisUtil.getJedisClient
        val today: String = new SimpleDateFormat("yyyy-MM-dd").format(new Date())
        println(today)
        val dauKey = "dau:" + today
        val midSet: util.Set[String] = jedis.smembers(dauKey)
        jedis.close()
        // 2、发
        val midBC: Broadcast[util.Set[String]] = ssc.sparkContext.broadcast(midSet)
        val filteredRDD: RDD[StartUpLog] = rdd.filter {
          startuplog => {
            // 3、executor收  筛查
            val midSet: util.Set[String] = midBC.value
            !midSet.contains(startuplog.mid)
          }
        }
        println("过滤后：" + filteredRDD.count())
        filteredRDD
      }
    }

    // 自检内部  分组去重：  0、转换成kv  1、先按mid进行分组  2、组内排序  按时间排序  3、取top1
    val groupbyMidDstream: DStream[(String, Iterable[StartUpLog])] = filteredDstream.map(startuplog => (startuplog.mid, startuplog)).groupByKey()
    val realFilteredDstream: DStream[StartUpLog] = groupbyMidDstream.map {
      case (mid, startuplogItr) => {
        val startuplogList: List[StartUpLog] = startuplogItr.toList.sortWith {
          (startuplog1, startuplog2) => {
            startuplog1.ts < startuplog2.ts // 正序排序
          }
        }
        val top1startuplog: List[StartUpLog] = startuplogList.take(1)
        top1startuplog(0)
      }
    }


    // 利用redis保存当日访问过的用户清单
    realFilteredDstream.foreachRDD {
      rdd => {
        // 将一个分区中rdd遍历循环  可以提高效率
        rdd.foreachPartition {
          startUpLogItr => {
//            val jedis: Jedis = new Jedis("hadoop102", 6379)
            val jedis: Jedis = RedisUtil.getJedisClient
            // 将每一个mid保存到redis中
            for (startUpLog <- startUpLogItr) {
              // 设置保存在redis中的key
              val daukey = "dau:" + startUpLog.logDate
              // 保存到jedis 格式为set
              println(startUpLog)
              jedis.sadd(daukey, startUpLog.mid)
              // 设置key保存时间
              jedis.expire(daukey, 60*60*24)
            }
            // 关闭jedis
            jedis.close()
          }
        }
      }
    }

    //把数据写入hbase+phoenix
    realFilteredDstream.foreachRDD{rdd=>
      rdd.saveToPhoenix("GMALL2020_DAU",
        Seq("MID", "UID", "APPID", "AREA", "OS", "CH", "TYPE", "VS", "LOGDATE", "LOGHOUR", "TS"),
        new Configuration,
        Some("hadoop102,hadoop103,hadoop104:2181"))
    }

    ssc.start()
    ssc.awaitTermination()

  }
}
