package com.atguigu.dw.gmall0105publisher.controller

import java.time.LocalDate

import com.atguigu.dw.gmall0105publisher.service.PublisherService
import org.json4s.jackson.JsonMethods
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.web.bind.annotation.{GetMapping, RequestParam, RestController}

import scala.collection.mutable


@RestController
class PublisherController {
    @Autowired
    var publishService: PublisherService = _
    
    @GetMapping(Array("/realtime-total"))
    def getRealtimeTotalDau(@RequestParam("date") date: String) = {
        val total: Long = publishService.getDauTotal(date)
        val orderTotalAmount = publishService.getOrderTotalAmount(date)
        
        val result =
            s"""
               |[
               |  {"id":"dau","name":"新增日活","value":$total},
               |  {"id":"new_mid","name":"新增用户","value":333},
               |  {"id":"order_amount","name":"新增销售额","value":$orderTotalAmount}
               |]
             """.stripMargin
        result
    }
    
    /*
    {
       "yesterday":{"11":383,"12":123,"17":88,"19":200 },
       "today":{"12":38,"13":1233,"17":123,"19":688 }
    }

     */
    @GetMapping(Array("/realtime-hour"))
    def getRealtimeHour(@RequestParam("id") id: String, @RequestParam("date") date: String) = {
        // 如果id是dau表示请求是查询小时的日活
        if (id == "dau") {
            val hour2CountTody: Map[String, Long] = publishService.getDauHour2CountMap(date)
            val hour2CountYesterday: Map[String, Long] = publishService.getDauHour2CountMap(getYesterday(date))
            
            var resultMap: Map[String, Map[String, Long]] = Map[String, Map[String, Long]]()
            resultMap += "today" -> hour2CountTody
            resultMap += "yesterday" -> hour2CountYesterday
            
            // 把前面的map转换json字符串
            import org.json4s.JsonDSL._
            JsonMethods.compact(JsonMethods.render(resultMap))
        } else if(id == "order_amount"){
    
            val hour2OrderSumTody: mutable.Map[String, Double] = publishService.getOrderHourTotalAmount(date)
            val hour2OrderSumYesterday: mutable.Map[String, Double] = publishService.getOrderHourTotalAmount(getYesterday(date))
    
            var resultMap: Map[String, mutable.Map[String, Double]] = Map[String, mutable.Map[String, Double]]()
            resultMap += "today" -> hour2OrderSumTody
            resultMap += "yesterday" -> hour2OrderSumYesterday
    
            // 把前面的map转换json字符串
            import org.json4s.JsonDSL._
            JsonMethods.compact(JsonMethods.render(resultMap))
        } else { // 查询的小时的新增用户
            null
        }
    }
    
    /**
      * 计算出来昨天
      *
      * @param date
      */
    def getYesterday(date: String) = {
        // 方法1:
        /*val formmater = new SimpleDateFormat("yyyy-MM-dd");
        val today = formmater.parse(date)
        val yesterday: Date = DateUtils.addDays(today, -1)
        formmater.format(yesterday)*/
        
        // 方法2:
        LocalDate.parse(date).minusDays(1).toString
    }
    
    
    
}
