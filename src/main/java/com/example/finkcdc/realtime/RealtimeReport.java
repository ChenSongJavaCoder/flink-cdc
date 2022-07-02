package com.example.finkcdc.realtime;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;


/**
 * @author: CS
 * @date: 2022/6/29 下午4:43
 * @description:
 */
@Slf4j
@Component
public class RealtimeReport {

    public static void main(String[] args) {
    }

//    @PostConstruct
    public void run() {
        Configuration configuration = new Configuration();
        configuration.setString(RestOptions.BIND_PORT, "8081");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration)
                .enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE)
                .setMaxParallelism(4)
                .setParallelism(1);
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);
        sqlModel(tEnv);
    }


    private static void sqlModel(TableEnvironment tEnv) {
        tEnv.executeSql("CREATE TABLE mysql_binlog (id INT NOT NULL, name STRING, age INT, PRIMARY KEY (id) NOT ENFORCED) " +
                "WITH ('connector' = 'mysql-cdc', " +
                "'hostname' = '127.0.0.1', " +
                "'port' = '3306', " +
                "'username' = 'root', " +
                "'password' = '', " +
                "'database-name' = 'mine', " +
                "'table-name' = 'student_[0-1]+'," +
                "'server-id'   = '1'" +
                ")");

        tEnv.executeSql("CREATE TABLE sink_table (name STRING, age INT, PRIMARY KEY (name) NOT ENFORCED)" +
                "WITH (" +
                "   'connector'  = 'jdbc'," +
                "   'url'        = 'jdbc:mysql://127.0.0.1:3306/mine'," +
                "   'table-name' = 'spend_report'," +
                "   'driver'     = 'com.mysql.cj.jdbc.Driver'," +
                "   'username'   = 'root'," +
                "   'password'   = ''" +
                ")");
        tEnv.executeSql("INSERT INTO sink_table SELECT  name, sum(age) as age FROM mysql_binlog group by name");
    }

//    private static void apiModel(TableEnvironment tEnv) {
//        // 创建source table
//        Schema sourceSchema = Schema.newBuilder()
//                .column("id", DataTypes.INT().notNull())
//                .column("name", DataTypes.STRING())
//                .column("age", DataTypes.INT())
//                .primaryKey("id")
//                .build();
//        tEnv.createTable("sourceTable", TableDescriptor.forConnector("mysql-cdc")
//                .schema(sourceSchema)
//                .option("hostname", "127.0.0.1")
//                .option("port", "3306")
//                .option("username", "root")
//                .option("password", "")
//                .option("database-name", "mine")
//                .option("table-name", "student")
//                .option("scan.incremental.snapshot.enabled", "true")
//                .build());
//        tEnv.from("sourceTable").printSchema();
//
//        // 创建sink table
//        Schema sinkSchema = Schema.newBuilder()
//                .column("name", DataTypes.STRING().notNull())
//                .column("age", DataTypes.INT())
//                .primaryKey("name")
//                .build();
//        tEnv.createTemporaryTable("sinkTable", TableDescriptor.forConnector("jdbc")
//                .schema(sinkSchema)
//                .option("url", "jdbc:mysql://127.0.0.1:3306/mine")
//                .option("driver", "com.mysql.cj.jdbc.Driver")
//                .option("table-name", "spend_report")
//                .option("username", "root")
//                .option("password", "")
//                .build());
//
//        // 输出
//        Table resultTable = tEnv.from("sourceTable");
//        report(resultTable).executeInsert("sinkTable");
//
//    }

}
