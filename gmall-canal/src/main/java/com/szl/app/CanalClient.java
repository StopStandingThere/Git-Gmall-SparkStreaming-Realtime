package com.szl.app;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.szl.constants.GmallConstants;
import com.szl.utils.MyKafkaSender;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Random;

public class CanalClient {
    public static void main(String[] args) throws InterruptedException, InvalidProtocolBufferException {
        //1.获取canal连接对象
        InetSocketAddress socketAddress = new InetSocketAddress("hadoop102", 11111);
        CanalConnector canalConnector = CanalConnectors.newSingleConnector(socketAddress, "example", "", "");

        while (true){
            //2.开启连接
            canalConnector.connect();

            //3.订阅数据库数据
            canalConnector.subscribe("gmall_realtime.*");

            //4.获取多个sql封装的数据
            Message message = canalConnector.get(100);

            //5.获取一个sql封装的数据
            List<CanalEntry.Entry> entries = message.getEntries();

            if (entries.size() <= 0){
                System.out.println("没有数据,休息一会儿!");
                Thread.sleep(5000);
            }else{
                //有数据,遍历entry集合获取到每一个entry
                for (CanalEntry.Entry entry : entries) {
                    //TODO 6.获取表名
                    String tableName = entry.getHeader().getTableName();

                    //7.获取entry类型
                    CanalEntry.EntryType entryType = entry.getEntryType();

                    //8.根据entry类型获取序列化数据
                    if(CanalEntry.EntryType.ROWDATA.equals(entryType)){

                        //9.获取序列化数据
                        ByteString storeValue = entry.getStoreValue();

                        //10.反序列化数据
                        CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(storeValue);

                        //TODO 11.获取事件类型
                        CanalEntry.EventType eventType = rowChange.getEventType();

                        //TODO 12.获取具体的数据
                        List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();

                        //根据表名以及事件类型获取不同的数据
                        handler(tableName,eventType,rowDatasList);
                    }
                }
            }
        }
    }

    private static void handler(String tableName, CanalEntry.EventType eventType, List<CanalEntry.RowData> rowDatasList) {
        //获取订单表新增数据
        if ("order_info".equals(tableName) && CanalEntry.EventType.INSERT.equals(eventType)){
            saveToKafka(rowDatasList, GmallConstants.KAFKA_TOPIC_ORDER);
        }else if ("order_detail".equals(tableName) && CanalEntry.EventType.INSERT.equals(eventType)){
            saveToKafka(rowDatasList, GmallConstants.KAFKA_TOPIC_ORDER_DETAIL);
        }else if ("user_info".equals(tableName) && (CanalEntry.EventType.INSERT.equals(eventType) || CanalEntry.EventType.UPDATE.equals(eventType))) {
            saveToKafka(rowDatasList, GmallConstants.KAFKA_TOPIC_USER);
        }
    }

    private static void saveToKafka(List<CanalEntry.RowData> rowDatasList, String kafkaTopicOrder) {
        for (CanalEntry.RowData rowData : rowDatasList) {
            List<CanalEntry.Column> afterColumnsList = rowData.getAfterColumnsList();
            //创建JSONObject用来存放每一列的列名和列值
            JSONObject jsonObject = new JSONObject();
            for (CanalEntry.Column column : afterColumnsList) {
                jsonObject.put(column.getName(), column.getValue());
            }
            System.out.println(jsonObject.toString());

            //模拟网络震荡
/*            try {
                Thread.sleep(new Random().nextInt(5000));
            } catch (InterruptedException e) {
                e.printStackTrace();
            }*/

            //将封装后的JSON字符串写入Kafka
            MyKafkaSender.send(kafkaTopicOrder, jsonObject.toString());
        }
    }
}
