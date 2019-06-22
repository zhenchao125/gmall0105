package com.atguigu.dw.gmall0105publisher.service

import java.util

import com.atguigu.dw.gmall0105.common.constant.GmallConstant
import io.searchbox.client.JestClient
import io.searchbox.core.search.aggregation.TermsAggregation
import io.searchbox.core.{Search, SearchResult}
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service

@Service
class PublisherServiceImp extends PublisherService {
    // 1. 连接es
    @Autowired
    private var jestClient: JestClient = _
    /**
      * 获取指定日期的日活数据
      *
      * @param date 指定的日期
      * @return 具体的日活数据
      */
    override def getDauTotal(date: String): Long = {
        val queryDSL =
            s"""
               |{
               |  "query": {
               |    "bool": {
               |      "filter": {
               |        "term": {
               |          "logDate": "$date"
               |        }
               |      }
               |    }
               |  }
               |}
             """.stripMargin
        //2. 查询
        val search: Search = new Search.Builder(queryDSL)
            .addIndex(GmallConstant.DAU_INDEX)
            .addType("_doc").build()
        val result: SearchResult = jestClient.execute(search)
        //3. 返回total
        result.getTotal.toLong
    }
    
    /**
      * 获取指定日期的小时统计的日活
      *
      * @param date
      * @return
      */
    override def getDauHour2CountMap(date: String): Map[String, Long] = {
        val queryDSL =
            s"""
               |{
               |  "query": {
               |    "bool": {
               |      "filter": {
               |        "term": {
               |          "logDate": "$date"
               |        }
               |      }
               |    }
               |  }
               |  ,"aggs": {
               |    "groupby_hour": {
               |      "terms": {
               |        "field": "logHour",
               |        "size": 24
               |      }
               |    }
               |  }
               |}
             """.stripMargin
        val search: Search = new Search.Builder(queryDSL)
            .addIndex(GmallConstant.DAU_INDEX)
            .addType("_doc")
            .build()
        val result: SearchResult = jestClient.execute(search)
        val buckets: util.List[TermsAggregation#Entry] = result.getAggregations.getTermsAggregation("groupby_hour").getBuckets
        
        var resultMap = Map[String, Long]()
        // 遍历每个bucket , 取出里面的key和count, 添加到最后的Map中
        for(i <- 0 until buckets.size){
            val bucket: TermsAggregation#Entry = buckets.get(i)
            resultMap += bucket.getKey -> bucket.getCount
        }
        resultMap
    }
    
    /**
      * 获取指定日期的销售总额
      *
      * @param date
      * @return
      */
    override def getOrderTotalAmount(date: String): Double = {
        val queryDSL =
            s"""
               |{
               |  "query": {
               |    "bool": {
               |        "filter": {
               |          "term": {
               |            "createDate": "$date"
               |          }
               |        }
               |      }
               |    },
               |    "aggs": {
               |      "sum_totalAmount": {
               |        "sum": {
               |          "field": "totalAmount"
               |        }
               |      }
               |    }
               |}
             """.stripMargin
        val search: Search = new Search.Builder(queryDSL)
            .addIndex(GmallConstant.ORDER_INDEX)
            .addType("_doc").build()
        
        val result: SearchResult = jestClient.execute(search)
        result.getAggregations.getSumAggregation("sum_totalAmount").getSum
    }
}
