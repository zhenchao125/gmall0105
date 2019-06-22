package com.atguigu.dw.gmall0105publisher.service

import java.util

import com.atguigu.dw.gmall0105.common.constant.GmallConstant
import io.searchbox.client.JestClient
import io.searchbox.core.search.aggregation.TermsAggregation
import io.searchbox.core.{Search, SearchResult}
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service

import scala.collection.mutable

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
        for (i <- 0 until buckets.size) {
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
    
    /**
      * 获取指定日期每个小时的销售额
      *
      * @param date
      * @return
      */
    override def getOrderHourTotalAmount(date: String): mutable.Map[String, Double] = {
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
               |      "groupby_createHour": {
               |        "terms": {
               |          "field": "createHour",
               |          "size": 24
               |        },
               |        "aggs": {
               |          "sum_totalAmount": {
               |            "sum": {
               |              "field": "totalAmount"
               |            }
               |          }
               |        }
               |      }
               |    }
               |}
             """.stripMargin
        
        val search: Search = new Search.Builder(queryDSL)
            .addIndex(GmallConstant.ORDER_INDEX)
            .addType("_doc").build()
        
        val result: SearchResult = jestClient.execute(search)
        
        val buckets: util.List[TermsAggregation#Entry] = result.getAggregations.getTermsAggregation("groupby_createHour").getBuckets
        import scala.collection.JavaConversions._
        val resultMap = mutable.Map[String, Double]()
        for (bucket <- buckets) {
            resultMap += bucket.getKey -> bucket.getSumAggregation("sum_totalAmount").getSum
        }
        resultMap
    }
    
    /**
      * 按照给定的条件从es中查询数据
      *
      * @param date
      * @param keyword
      * @param startPage
      * @param size
      * @param aggField
      * @param aggSize 年龄: 100   性别: 2
      */
    override def getSaleDetailAndAggResultByField(date: String, keyword: String, startPage: Int, size: Int, aggField: String, aggSize: Int): Map[String, Any] = {
        val queryDSL =
            s"""
               |{
               |  "from": ${(startPage - 1) * size},
               |  "size": $size,
               |  "query": {
               |    "bool": {
               |      "filter": {
               |        "term": {
               |          "dt": "$date"
               |        }
               |      }
               |      , "must": [
               |        {"match": {
               |          "sku_name": {
               |            "query": "$keyword",
               |            "operator": "and"
               |          }
               |        }}
               |      ]
               |    }
               |  }
               |  , "aggs": {
               |    "groupby_$aggField": {
               |      "terms": {
               |        "field": "$aggField",
               |        "size": $aggSize
               |      }
               |    }
               |  }
               |}
            """.stripMargin
        
        val search: Search = new Search.Builder(queryDSL)
            .addIndex(GmallConstant.SALE_DETAIL_INDEX)
            .addType("_doc")
            .build()
        val result: SearchResult = jestClient.execute(search)
        
        // 3个键值对:  总数, 明细, 聚合数据  total -> 100,  "detail" -> List[Map[String, Any]],  "aggMap" -> Map[String, Double]
        var resultMap: Map[String, Any] = Map[String, Any]()
        // 1. 得到命中的总数
        resultMap += "total" -> result.getTotal
        
        // 2. 得到销售明细
        val hits: util.List[SearchResult#Hit[util.HashMap[String, Any], Void]] =
            result.getHits(classOf[util.HashMap[String, Any]])
        import scala.collection.JavaConversions._
        var detailList: List[Map[String, Any]] = List[Map[String, Any]]() // 存储明细
        for (hit <- hits) {
            val source: util.HashMap[String, Any] = hit.source
            detailList = source.toMap :: detailList
        }
        resultMap += "detail" -> detailList
        
        // 3. 得到聚合的数据
        var aggMap = Map[String, Double]()
        val buckets: util.List[TermsAggregation#Entry] =
            result.getAggregations.getTermsAggregation(s"groupby_$aggField").getBuckets
        println(buckets.size())
        for (bucket <- buckets) {
            aggMap += bucket.getKey -> bucket.getCount.toDouble
        }
        resultMap += "aggMap" -> aggMap
    
        // 4. 返回最终结果
        resultMap
    }
}
