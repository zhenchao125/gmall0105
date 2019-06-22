package com.atguigu.dw.gmall0105publisher.controller

import java.text.DecimalFormat
import java.time.LocalDate

import com.atguigu.dw.gmall0105publisher.bean.{Opt, SaleInfo, Stat}
import com.atguigu.dw.gmall0105publisher.service.PublisherService
import org.json4s.DefaultFormats
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
        } else if (id == "order_amount") {
            
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
    
    /*
     http://localhost:8070/sale_detail?date=2019-05-20&&startpage=1&&size=5&&keyword=手机小米
     */
    @GetMapping(Array("/sale_detail"))
    def saleDetail(@RequestParam("date") date: String,
                   @RequestParam("startpage") startPage: Int,
                   @RequestParam("size") size: Int,
                   @RequestParam("keyword") keyword: String) = {
        val formatter = new DecimalFormat(".00")
        // 1. 按照性别查询
        val genderMap: Map[String, Any] = publishService.getSaleDetailAndAggResultByField(date, keyword, startPage, size, "user_gender", 2)
        val total = genderMap("total").asInstanceOf[Integer]
        var title = "男女比例数据分析"
        var aggMap = genderMap("aggMap").asInstanceOf[Map[String, Double]] // F -> 100, M -> 100
        
        val femaleCount: Double = aggMap("F")
        val maleCount: Double = aggMap("M")
        val genderOptList = List[Opt](
            Opt("男", formatter.format(maleCount / total)),
            Opt("女", formatter.format(femaleCount / total))/*,
            Opt("total", total.toString)*/
        )
        // 表示性别比例的饼图
        val genderStat = Stat(title, genderOptList)
        
        // 2. 按照年龄查, 还要年龄段的处理
        val ageMap: Map[String, Any] = publishService.getSaleDetailAndAggResultByField(date, keyword, startPage, size, "user_age", 100)
        title = "年龄比例数据分析"
        aggMap = ageMap("aggMap").asInstanceOf[Map[String, Double]]
        val temp1: Map[String, Map[String, Double]] = aggMap.groupBy {
            case (age, _) => {
                if (age.toInt <= 20) "20岁以下"
                else if (age.toInt > 20 && age.toInt <= 30) "20岁到30岁"
                else "30岁以上"
            }
        }
        val optList: List[Opt] = temp1.map {
            case (ageRange, map) => {
                (ageRange, map.foldLeft(0D)(_ + _._2))
            }
        }.map {
            case (ageRange, count) => {
                Opt(ageRange, formatter.format(count / total))
            }
        }.toList
        val ageStat = Stat(title, optList)
        // 3. 详表
        val detailList = genderMap("detail").asInstanceOf[List[Map[String, Any]]]
        println(detailList.size)
        
        // 4. 返回的最终数据模型
        val saleInfo: SaleInfo = SaleInfo(total, genderStat:: ageStat ::Nil, detailList)
        
        // 5. 转成json返回给请求者
        import org.json4s.jackson.Serialization
        implicit val formats: DefaultFormats.type = DefaultFormats
        Serialization.write(saleInfo)
    }
    
    
}
