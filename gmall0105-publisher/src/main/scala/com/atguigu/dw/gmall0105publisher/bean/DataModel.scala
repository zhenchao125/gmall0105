package com.atguigu.dw.gmall0105publisher.bean


import org.json4s.JValue

/**
  * 封装饼图中的数据
  * 代表一张饼图
  *
  * 一个标题 和 多个 Opt
  */
case class Stat(title: String, options: List[Opt])

/**
  * 表示结果中的一个选项
  * 男  0.1 ...
  * *
  * 20-20岁  0.2
  */
case class Opt(name: String, value: String)

/**
  * 封装返回给前端的所有数据
  *
  * 总量
  *
  * stats: 多张饼图
  *
  * detail: 详情
  */
case class SaleInfo(total: Int, stats: List[Stat], detail: List[Map[String, Any]])

/*
{
 "total": 62,
 "stat": [{
     "options": [{
         "name": "20岁以下",
         "value": 0.0
     }, {
         "name": "20岁到30岁",
         "value": 25.8
     }, {
         "name": "30岁及30岁以上",
         "value": 74.2
     }],
     "title": "用户年龄占比"
 }, {
     "options": [{
         "name": "男",
         "value": 38.7
     }, {
         "name": "女",
         "value": 61.3
     }],
     "title": "用户性别占比"
 }],
 "detail": [{
     "user_id": "9",
     "sku_id": "8",
     "user_gender": "M",
     "user_age": 49.0,
     "user_level": "1",
     "sku_price": 8900.0,
     "sku_name": "Apple iPhone XS Max (A2104) 256GB 深空灰色 移动联通电信4G手机 双卡双待",
     "sku_tm_id": "86",
     "sku_category1_id": "2",
     "sku_category2_id": "13",
     "sku_category3_id": "61",
     "sku_category1_name": "手机",
     "sku_category2_name": "手机通讯",
     "sku_category3_name": "手机",
     "spu_id": "1",
     "sku_num": 6.0,
     "order_count": 2.0,
     "order_amount": 53400.0,
     "dt": "2019-05-20",
     "es_metadata_id": "wPdM7GgBQMmfy2BJr4YT"
 }, {
     "user_id": "5",
     "sku_id": "8",
     "user_gender": "F",
     "user_age": 36.0,
     "user_level": "4",
     "sku_price": 8900.0,
     "sku_name": "Apple iPhone XS Max (A2104) 256GB 深空灰色 移动联通电信4G手机 双卡双待",
     "sku_tm_id": "86",
     "sku_category1_id": "2",
     "sku_category2_id": "13",
     "sku_category3_id": "61",
     "sku_category1_name": "手机",
     "sku_category2_name": "手机通讯",
     "sku_category3_name": "手机",
     "spu_id": "1",
     "sku_num": 5.0,
     "order_count": 1.0,
     "order_amount": 44500.0,
     "dt": "2019-05-20",
     "es_metadata_id": "wvdM7GgBQMmfy2BJr4YT"
 }, {
     "user_id": "19",
     "sku_id": "8",
     "user_gender": "F",
     "user_age": 43.0,
     "user_level": "5",
     "sku_price": 8900.0,
     "sku_name": "Apple iPhone XS Max (A2104) 256GB 深空灰色 移动联通电信4G手机 双卡双待",
     "sku_tm_id": "86",
     "sku_category1_id": "2",
     "sku_category2_id": "13",
     "sku_category3_id": "61",
     "sku_category1_name": "手机",
     "sku_category2_name": "手机通讯",
     "sku_category3_name": "手机",
     "spu_id": "1",
     "sku_num": 7.0,
     "order_count": 2.0,
     "order_amount": 62300.0,
     "dt": "2019-05-20",
     "es_metadata_id": "xvdM7GgBQMmfy2BJr4YU"
 }, {
     "user_id": "15",
     "sku_id": "8",
     "user_gender": "M",
     "user_age": 66.0,
     "user_level": "4",
     "sku_price": 8900.0,
     "sku_name": "Apple iPhone XS Max (A2104) 256GB 深空灰色 移动联通电信4G手机 双卡双待",
     "sku_tm_id": "86",
     "sku_category1_id": "2",
     "sku_category2_id": "13",
     "sku_category3_id": "61",
     "sku_category1_name": "手机",
     "sku_category2_name": "手机通讯",
     "sku_category3_name": "手机",
     "spu_id": "1",
     "sku_num": 3.0,
     "order_count": 1.0,
     "order_amount": 26700.0,
     "dt": "2019-05-20",
     "es_metadata_id": "xvdM7GgBQMmfy2BJr4YU"
 }]
}

 */