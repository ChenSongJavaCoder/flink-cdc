package com.example.finkcdc;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

/**
 * @author: CS
 * @date: 2022/6/28 下午4:13
 * @description:
 */
@Slf4j
public class MySink implements SinkFunction<String> {

    @Override
    public void invoke(String value, Context context) {
        log.info("新数据:{}", value);
    }
}
