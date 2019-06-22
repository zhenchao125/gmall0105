package com.atguigu.dw.gmall0105publisher.service

trait PublisherService {
    /**
      * 获取指定日期的日活数据
      *
      * @param date 指定的日期
      * @return 具体的日活数据
      */
    def getDauTotal(date: String): Long
    
    /**
      * 获取指定日期的小时统计的日活
      * @param date
      * @return
      */
    def getDauHour2CountMap(date: String): Map[String, Long]
    
    /**
      * 获取指定日期的销售总额
      * @param date
      * @return
      */
    def getOrderTotalAmount(date: String): Double
}
