//package com.example.finkcdc;
//
//import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
//import io.debezium.data.Envelope;
//import lombok.extern.slf4j.Slf4j;
//import org.apache.flink.api.common.typeinfo.TypeInformation;
//import org.apache.flink.util.Collector;
//import org.apache.kafka.connect.data.Struct;
//import org.apache.kafka.connect.source.SourceRecord;
//
//@Slf4j
//public class MyDeserialization implements DebeziumDeserializationSchema<String> {
//    @Override
//    public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {
//        //获取主题信息,包含着数据库和表名
//        String topic = sourceRecord.topic();
//        String[] arr = topic.split("\\.");
//        String db = arr[1];
//        String tableName = arr[2];
//        //获取操作类型 READ DELETE UPDATE CREATE
//        Envelope.Operation operation =
//                Envelope.operationFor(sourceRecord);
//        //获取值信息并转换为 Struct 类型
//        Struct value = (Struct) sourceRecord.value();
//        log.info("读取到数据：database:{} table:{} operation:{} data:{}", db, tableName, operation, value);
//        collector.collect(value.toString());
//    }
//
//    @Override
//    public TypeInformation<String> getProducedType() {
//        return TypeInformation.of(String.class);
//    }
//}
