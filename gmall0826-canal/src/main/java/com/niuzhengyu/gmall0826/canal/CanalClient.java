package com.niuzhengyu.gmall0826.canal;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.net.InetSocketAddress;
import java.util.List;

public class CanalClient {

    public static void main(String[] args) {
        // 1、连接canal的server端
        CanalConnector canalConnector = CanalConnectors.newSingleConnector(new InetSocketAddress("hadoop102", 11111), "example", "", "");

        // 需要用到一个死循环让canal不停的连接server
        while (true) {
            canalConnector.connect();

            // 2、抓取数据
            canalConnector.subscribe("*.*");
            // 1个message对象包含了100个什么？
            // 100个sql单位  1个sql单位 = 一个sql执行后影响的row集合
            Message message = canalConnector.get(100); // 一次性获取100个sql单位
            // 判断message中获取到数据没
            if (message.getEntries().size() == 0) {
                System.out.println("没有数据，休息一会!");
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            } else { // 有数据 entry = sql单位
                // 3、把抓取到的数据展开，变成想要的格式
                // 因为message.getEntries()是多个  所以遍历循环转变
                for (CanalEntry.Entry entry : message.getEntries()) {
                    // 当entry是ROWDATA类型的时候进去这个语句
                    if (CanalEntry.EntryType.ROWDATA == entry.getEntryType()) {
                        // 获取到这个entry的值
                        // protobuf是一个压缩算法  ByteString 是这个算法中自定义的
                        // storeValue是一个压缩的文件 需要反序列化才能读取
                        ByteString storeValue = entry.getStoreValue();
                        // rowchange是结构化的，反序列化的sql单位
                        CanalEntry.RowChange rowChange = null;
                        try {
                            // 反序列化工具
                            rowChange = CanalEntry.RowChange.parseFrom(storeValue);
                        } catch (InvalidProtocolBufferException e) {
                            e.printStackTrace();
                        }
                        // 得到行集合
                        List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();
                        // 得到操作类型
                        CanalEntry.EventType eventType = rowChange.getEventType();
                        // 得到表名
                        String tableName = entry.getHeader().getTableName();
                        // 4、根据不同的业务类型发送到不同的kafka
                        CanalHandler canalHandler = new CanalHandler(eventType, tableName, rowDatasList);
                        canalHandler.handle();
                    }
                }
            }
        }
    }
}
