package com.atguigu.dw.gmall0105.realtime.app

import com.alibaba.fastjson.JSON
import com.atguigu.dw.gmall0105.common.constant.GmallConstant
import com.atguigu.dw.gmall0105.common.util.MyESUtil
import com.atguigu.dw.gmall0105.realtime.bean.OrderInfo
import com.atguigu.dw.gmall0105.realtime.util.MyKafkaUtil
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Author lzc
  * Date 2019-06-22 08:47
  */
object OrderApp {
    def main(args: Array[String]): Unit = {
        
        val conf = new SparkConf().setMaster("local[2]").setAppName("OrderApp")
        val ssc = new StreamingContext(conf, Seconds(5))
        val sourceDStream: InputDStream[(String, String)] = MyKafkaUtil.getKafkaStream(ssc, GmallConstant.TOPIC_ORDER)
        // 1. 调整数据结构
        val orderInfoDStream: DStream[OrderInfo] = sourceDStream.map {
            case (_, jsonString) => {
                val orderInfo: OrderInfo = JSON.parseObject(jsonString, classOf[OrderInfo])
                // 对收件人和收件人的电话做脱敏处理
                orderInfo.consignee = orderInfo.consignee.substring(0, 2) + "***"
                orderInfo.consigneeTel = orderInfo.consigneeTel.substring(0, 3) + "****" +
                    orderInfo.consigneeTel.substring(7, 11)
            
                orderInfo
            }
        }
        orderInfoDStream.foreachRDD(rdd => {
            rdd.foreachPartition(it => {
                // 2. 写入到es
                MyESUtil.insertBulk(GmallConstant.ORDER_INDEX, it.toIterable)
            })
        })
        
        ssc.start()
        ssc.awaitTermination()
    }
}

/*
1. 从kafka读数据
    1. ssc
    2. dstream
    3. ...

2. 写入到es
    
    两种格式的数据:
        1. json格式
        2. 样例类对象
        
3. 统计每个小时的交易额
    hour...
    
 */