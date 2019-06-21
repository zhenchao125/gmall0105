package com.atguigu.dw.gmall0105publisher.service

import com.atguigu.dw.gmall0105.common.constant.GmallConstant
import io.searchbox.client.JestClient
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
    override def getDauHour2CountMap(date: String): Map[String, Long] = ???
}
