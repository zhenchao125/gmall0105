package com.atguigu.gmall0105.canal.util

import java.util

import com.alibaba.fastjson.JSONObject
import com.alibaba.otter.canal.protocol.CanalEntry
import com.alibaba.otter.canal.protocol.CanalEntry.EventType
import com.atguigu.dw.gmall0105.common.constant.GmallConstant
import com.google.common.base.CaseFormat

/**
  * Author lzc
  * Date 2019-06-21 15:47
  */
object CanalHandler {
    // 真正的处理canal来的数据
    def handle(tableName: String, eventType: EventType, rowDataList: util.List[CanalEntry.RowData]) = {
        // 1. 把数据给解析出来
        if (tableName == "order_info" && eventType == EventType.INSERT && rowDataList.size() > 0) {
            import scala.collection.JavaConversions._
            for (rowData <- rowDataList) {
                val jsonObj = new JSONObject()
                val columnsList: util.List[CanalEntry.Column] = rowData.getAfterColumnsList
                for (column <- columnsList) {
                    // 下划线命名转成驼峰命名
                    val temp: String = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, column.getName)
                    jsonObj.put(temp, column.getValue)
                }
                // 2. 然后写入到 kafka  key:null  value: {cn: cv, cn: cv}
                MyKafkaSender.send(GmallConstant.TOPIC_ORDER, jsonObj.toJSONString)
            }
        }
        
        
        
        //
    }
}
