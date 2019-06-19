package com.atguigu.dw.gmall0105.common.util
import io.searchbox.client.config.HttpClientConfig
import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.core.{Bulk, Index}

/**
  * Author lzc
  * Date 2019-06-19 15:17
  */
object MyESUtil {
    val es_host = "http://hadoop201"
    val es_port = 9200
    val esClientFactory = buildFactory
    
    // 得到一个客户端工厂
    def buildFactory = {
        val config: HttpClientConfig = new HttpClientConfig.Builder(s"$es_host:$es_port")
            .multiThreaded(true)
            .maxTotalConnection(20)
            .connTimeout(10000)
            .readTimeout(10000)
            .build()
        
        val factory = new JestClientFactory
        factory.setHttpClientConfig(config)
        factory
    }
    
    // 通过客户端工厂得到客户端. 通过客户端就可以向es写入数据
    def getESClient = esClientFactory.getObject
    
    
    /*
    单条测试
     */
    def singleOperation() = {
        val client: JestClient = getESClient
        
        // index type   document
        // 2种document: 1种是json格式  1种样例类形式
        
        //1. json格式
        val source1 =
            s"""
               |{
               |    "name": "lisi",
               |    "age": 20
               |}
             """.stripMargin
        
        
        // 2. 样例类格式
        val source2 = User("ww", 30)
        
        val indeBuilder: Index.Builder = new Index.Builder(source2)
            .index("user")
            .`type`("_doc")
        
        
        client.execute(indeBuilder.build())
        
        closeClient(client)
    }
    
    /**
      * 测试批量插入
      *
      */
    def mutltiOperation() = {
        val client: JestClient = getESClient
        
        val user1 = User("aa", 30)
        val user2 = User("bb", 30)
        
        val bulk: Bulk = new Bulk.Builder()
            .defaultIndex("user")
            .defaultType("_doc")
            .addAction(new Index.Builder(user1).build())
            .addAction(new Index.Builder(user2).build())
            .build()
        client.execute(bulk)
        closeClient(client)
    }
    
    def main(args: Array[String]): Unit = {
        //        singleOperation()
        //        mutltiOperation()
    }
    
    /**
      * 向 指定的index中,插入指定数据集
      *
      * @param index
      * @param sources
      */
    def insertBulk(index: String, sources: Iterable[Any]): Unit = {
        val bulkBuilder: Bulk.Builder = new Bulk.Builder().defaultIndex(index).defaultType("_doc")
        sources.foreach(source => {
            bulkBuilder.addAction(new Index.Builder(source).build())
        })
        val client: JestClient = getESClient
        client.execute(bulkBuilder.build())
        closeClient(client)
    }
    
    /**
      * 关闭连接es的客户端
      *
      * @param client
      */
    def closeClient(client: JestClient) = {
        if (client != null) {
            try {
                client.shutdownClient()
            } catch {
                case e => e.printStackTrace()
            }
        }
    }
}

case class User(name: String, age: Int)