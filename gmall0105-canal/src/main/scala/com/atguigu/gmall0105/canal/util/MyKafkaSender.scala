package com.atguigu.gmall0105.canal.util

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

/**
  * Author lzc
  * Date 2019-06-21 16:51
  */
object MyKafkaSender {
    val props = new Properties()
    // Kafka服务端的主机名和端口号
    props.put("bootstrap.servers", "hadoop201:9092,hadoop202:9092,hadoop203:9093")
    // key序列化
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    // value序列化
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    
    private val producer = new KafkaProducer[String, String](props)
    
    def send(topic: String, content: String) = {
        // 发送数据
        producer.send(new ProducerRecord[String, String](topic, content))
    }
}
