package com.niuzhengyu.gmall0826.canal;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.niuzhengyu.gmall0826.canal.util.MyKafkaSender;
import com.niuzhengyu.gmall0826.common.constant.GmallConstant;

import java.util.List;

public class CanalHandler {

    CanalEntry.EventType eventType;

    String tableName;

    List<CanalEntry.RowData> rowDataList;

    public CanalHandler(CanalEntry.EventType eventType, String tableName, List<CanalEntry.RowData> rowDataList) {
        this.eventType = eventType;
        this.tableName = tableName;
        this.rowDataList = rowDataList;
    }

    // 对行集合进行处理
    public void handle() {

        // 如果行集合不为空切长度大于0
        if (this.rowDataList != null && this.rowDataList.size() > 0) {
            // 下单操作
            if (tableName.equals("order_info") && eventType == CanalEntry.EventType.INSERT) {
                sendKafka(rowDataList, GmallConstant.KAFKA_TOPIC_ORDER);
            }
        }
    }

    public void sendKafka(List<CanalEntry.RowData> rowDataList, String topic) {
        // 遍历
        for (CanalEntry.RowData rowData : rowDataList) {
            // 因为是下单操作，所以只能获取到插入操作之后的数据
            List<CanalEntry.Column> afterColumnsList = rowData.getAfterColumnsList();
            JSONObject jsonObject = new JSONObject();
            for (CanalEntry.Column column : afterColumnsList) {
                // 将获取的数据进行拼接
                System.out.println(column.getName() + "::" + column.getValue());
                jsonObject.put(column.getName(), column.getValue());
            }
            String jsonString = jsonObject.toJSONString();
            MyKafkaSender.send(GmallConstant.KAFKA_TOPIC_ORDER, jsonString);
        }
    }
}
