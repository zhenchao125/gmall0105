package com.atguigu.dw.gmall0105.realtime.app

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.dw.gmall0105.common.constant.GmallConstant
import com.atguigu.dw.gmall0105.realtime.bean.StartupLog
import com.atguigu.dw.gmall0105.realtime.util.{MyKafkaUtil, RedisUtil}
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

/**
  * Author lzc
  * Date 2019-06-19 09:09
  */
object DauApp {
    def main(args: Array[String]): Unit = {
        // 1. 从kafka消费数据
        val conf: SparkConf = new SparkConf().setAppName("DauApp").setMaster("local[2]")
        val ssc = new StreamingContext(conf, Seconds(5))
        val sourceDSream: InputDStream[(String, String)] = MyKafkaUtil.getKafkaStream(ssc, GmallConstant.TOPIC_STARTUP)
        
        // 2. 使用redis清洗, 去重
        val startupLogDStream: DStream[StartupLog] = sourceDSream.map {
            case (_, log) => JSON.parseObject(log, classOf[StartupLog])
        }
        
        
        //2.1负责根据已经启动过的uid, 把这些给过滤掉
        val filteredDStream: DStream[StartupLog] = startupLogDStream.transform(rdd => {
            // driver中执行的代码
            
            // 如果有多个相同的uid的记录应该只留下一个记录
            val groupedRDD: RDD[(String, Iterable[StartupLog])] = rdd.groupBy(_.uid)
            // uid相同的去重
            val distincedRDD: RDD[StartupLog] = groupedRDD.flatMap {
                case (_, it) => it.take(1)
            }
            
            val client: Jedis = RedisUtil.getJedisClient
            val uidsSet: util.Set[String] = client.smembers(GmallConstant.REDIS_DAU_KEY + ":" + new SimpleDateFormat("yyyy-MM-dd").format(new Date()))
            client.close()
            val uidsSetBC: Broadcast[util.Set[String]] = ssc.sparkContext.broadcast(uidsSet)
            distincedRDD.filter(startupLog => {
                // executor中执行的代码
                !uidsSetBC.value.contains(startupLog.uid)
            })
        })
        
        
        // 2.2负责把过滤后的写入到 redis
        filteredDStream.foreachRDD(rdd => {
            rdd.foreachPartition(startupLogIt => {
                val client: Jedis = RedisUtil.getJedisClient
                // 把启动日志的uid写入到redis
                startupLogIt.foreach(startupLog => {
                    client.sadd(GmallConstant.REDIS_DAU_KEY + ":" + startupLog.logDate, startupLog.uid)
                })
                client.close()
                
                // 3. 写到 es 中
                
                
            })
        })
        
        
        ssc.start()
        ssc.awaitTermination()
        
    }
}

/*
1. 从kafka消费数据


2. 使用redis清洗, 去重


3. 写入到es

 */