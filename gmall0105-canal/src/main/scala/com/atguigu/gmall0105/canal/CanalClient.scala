package com.atguigu.gmall0105.canal

import java.net.InetSocketAddress

import com.alibaba.otter.canal.client.{CanalConnector, CanalConnectors}
import com.alibaba.otter.canal.protocol.CanalEntry.{EntryType, RowChange}
import com.alibaba.otter.canal.protocol.{CanalEntry, Message}
import com.atguigu.gmall0105.canal.util.CanalHandler
import com.google.protobuf.ByteString
import java.util.{List => JavaList}

/**
  * Author lzc
  * Date 2019-06-21 15:25
  */
object CanalClient {
    def main(args: Array[String]): Unit = {
        
        var address = new InetSocketAddress("hadoop201", 11111)
        // 1. 到canal的连接器对象
        val connector: CanalConnector = CanalConnectors.newSingleConnector(address, "example", "", "")
        // 2. 连接到canal
        connector.connect()
        // 3. 订阅指定表的变化
        connector.subscribe("gmall.order_info")
        
        while (true) {
            // 4. 获取 message
            val msg: Message = connector.get(100)
            //5. 获取到每行数据组成的集合
            val entries: JavaList[CanalEntry.Entry] = msg.getEntries
            import scala.collection.JavaConversions._  // 因为scala的for不能遍历java的集合, 所以需要添加隐式转换来支持
            if (entries.size() > 0) {
                // 6. 遍历出来每一行数据
                for (entry <- entries) {
                    if (entry.getEntryType == EntryType.ROWDATA) {
                        // 7. 获取到每行数据
                        val value: ByteString = entry.getStoreValue
                        val rowChange: RowChange = RowChange.parseFrom(value)
                        val list: JavaList[CanalEntry.RowData] = rowChange.getRowDatasList
                        // 8. 真正的处理数据  参数1:表名 参数2: 事件类型, 插入, 删除..
                        CanalHandler.handle(entry.getHeader.getTableName, rowChange.getEventType, list)
                    }
                }
            }else{
                println("没有拉取到数据, 1s后重新拉取")
                Thread.sleep(1000)
            }
        }
    }
}
