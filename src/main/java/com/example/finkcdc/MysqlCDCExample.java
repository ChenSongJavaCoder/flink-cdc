package com.example.finkcdc;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

/**
 * @author: CS
 * @date: 2022/6/28 下午2:21
 * @description:
 */
public class MysqlCDCExample {

    public static void main(String[] args) throws Exception {
        //创建监听的数据源
//        SourceFunction<String> sourceFunction = MySQLSource.<String>builder()
//                .hostname("127.0.0.1")
//                .port(3306)
//                // 监听test数据库下的所有表
//                .databaseList("mine")
//                //这里一定要带库名，因为上面可以同时监听多个库，会有不同库相同表名的情况
//                .tableList("mine.student")
//                .username("root")
//                .password("")
//                // 将SourceRecord 转换成 String形式
//                .deserializer(new MyDeserialization())
//                .build();
        //获取运行环境

        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        TableEnvironment tableEnvironment = TableEnvironment.create(settings);
        tableEnvironment.executeSql("CREATE TABLE mysql_binlog (id INT NOT NULL, name STRING, age INT) WITH ('connector' = 'mysql-cdc', 'hostname' = '127.0.0.1', 'port' = '3306', 'username' = 'root', 'password' = '', 'database-name' = 'mine', 'table-name' = 'student')");
        // 创建下游数据表，这里使用print类型的connector，将数据直接打印出来
        tableEnvironment.executeSql("CREATE TABLE sink_table (name STRING, age INT, PRIMARY KEY (name) NOT ENFORCED) " +
                "WITH (\n" +
                "   'connector'  = 'jdbc',\n" +
                "   'url'        = 'jdbc:mysql://127.0.0.1:3306/mine',\n" +
                "   'table-name' = 'spend_report',\n" +
                "   'driver'     = 'com.mysql.cj.jdbc.Driver',\n" +
                "   'username'   = 'root',\n" +
                "   'password'   = ''\n" +
                ")");
        // 将CDC数据源和下游数据表对接起来
        tableEnvironment.executeSql("INSERT INTO sink_table SELECT  name, sum(age) FROM mysql_binlog group by name");

//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1)
//                .enableCheckpointing(50000, CheckpointingMode.EXACTLY_ONCE)
//        ;
//        //开始监听数据,并将动态数据变化打印到屏幕中
//        env.addSource(sourceFunction)
//                .addSink(new MySink()).setParallelism(1)
//        ;
//
//        //开始执行任务
//        env.execute("mine-table-sync-test");
    }

}
