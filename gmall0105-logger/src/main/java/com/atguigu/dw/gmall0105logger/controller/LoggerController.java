package com.atguigu.dw.gmall0105logger.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * @Author lzc
 * @Date 2019-06-18 11:13
 */
@RestController
public class LoggerController {

    @PostMapping("/log")   // http://localhost:8080/log    log="{...}"
    public String doLog(@RequestParam("log") String log) {
        JSONObject jsonObj = JSON.parseObject(log);
        // 1. 添加事件戳
        jsonObj = addTS(jsonObj);
        // 2. 落盘
        saveLog(jsonObj);

        // 3. 写入到kafka

        return "success";
    }

    /**
     * 给 参数添加时间戳
     * @param jsonObj
     * @return 返回带时间戳的 JSONObject对象
     */
    public JSONObject addTS(JSONObject jsonObj) {
        jsonObj.put("ts", System.currentTimeMillis());
        return jsonObj;
    }

    // 初始化 Logger 对象
    private final Logger logger = LoggerFactory.getLogger(LoggerController.class);
    /**
     * 日志落盘
     * 使用 log4j
     * @param logObj
     */
    public void saveLog(JSONObject logObj) {
        logger.info(logObj.toJSONString());
    }

}

/*
1. 给每条日志添加一个时间戳

2. 把日志落盘(落盘服务某个文件中)

3. 把日志写入到kafka

 */
