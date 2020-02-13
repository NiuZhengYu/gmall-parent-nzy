package com.niuzhengyu.gmall0826.logger.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.niuzhengyu.gmall0826.common.constant.GmallConstant;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;

//@Controller
@RestController  // @RestController = @Controller + @ResponseBody
@Slf4j
public class LoggerController {

    @Autowired
    KafkaTemplate kafkaTemplate;

    // 获取的日志路径 http://logserver/log?logstring={jsonlog}

    //@RequestMapping(value = "log",method = RequestMethod.POST)
    @PostMapping("log") // 请求的类型为Post
    //@ResponseBody
    public String doLog(@RequestParam("logString") String logString) {
        System.out.println(logString);
        // 加时间戳
        JSONObject jsonObject = JSON.parseObject(logString);
        jsonObject.put("ts", System.currentTimeMillis());

        // 本地落盘成日志文件
        String logJsonString = jsonObject.toJSONString();
        log.info(logJsonString);

        // 发送kafka
        if ("startup".equals(jsonObject.getString("type"))) {
            kafkaTemplate.send(GmallConstant.KAFKA_TOPIC_STARTUP, logJsonString);
        } else {
            kafkaTemplate.send(GmallConstant.KAFKA_TOPIC_EVENT, logJsonString);
        }
        return "success";
    }
}
