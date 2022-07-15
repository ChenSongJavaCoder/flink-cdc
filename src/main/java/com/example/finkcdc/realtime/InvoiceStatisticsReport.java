package com.example.finkcdc.realtime;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.StateBackendOptions;
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
public class InvoiceStatisticsReport {

    static String sourceTable = "CREATE TABLE `source_kp_output_invoice` (\n" +
            "  `kp_output_invoice_id` STRING NOT NULL COMMENT '销项发票id',\n" +
            "  `custom_id` STRING  COMMENT '企业id',\n" +
            "  `invoice_code` STRING  COMMENT '发票代码',\n" +
            "  `invoice_number` STRING  COMMENT '发票号码',\n" +
            "  `invoice_date` STRING  COMMENT '开票日期',\n" +
            "  `invoice_state` STRING  COMMENT '发票状态 1：正常 2:红冲 3：蓝字作废 4：红字作废 5：空白作废  静态数据配置表查询数据项\\r\\n对应静态数据表 business_name=output_invoice sub_business_name=invoice_state',\n" +
            "  `sellers_tax_number` STRING  COMMENT '销售方税号',\n" +
            "  `sellers_tax_name` STRING  COMMENT '销售方名称',\n" +
            "  `buyers_tax_number` STRING  COMMENT '购买方税号',\n" +
            "  `buyers_tax_name` STRING  COMMENT '购买方名称',\n" +
            "  `price` DECIMAL(18,2)  COMMENT '金额',\n" +
            "  `tax_amount` DECIMAL(18,2)  COMMENT '税额',\n" +
            "  `total_price_tax` DECIMAL(18,2)  COMMENT '价税合计',\n" +
            "  `check_code` STRING  COMMENT '校验码 长度二十 每五位加个空格',\n" +
            "  `sellers_address` STRING  COMMENT '销售方地址',\n" +
            "  `sellers_phone_number` STRING  COMMENT '销售方电话',\n" +
            "  `sellers_bank_and_account_number` STRING  COMMENT '销售方开户行及账号',\n" +
            "  `buyers_address_and_telephone_number` STRING  COMMENT '购买方地址、电话',\n" +
            "  `buyers_bank_and_account_number` STRING  COMMENT '购买方开户行及账号',\n" +
            "  `password_area` STRING  COMMENT '密码区 非汉字防伪企业：长度108位。\\r\\n            \\r\\n            汉字防伪企业：密文显示防伪二维码，一般4个二维码。（本期暂不考虑）',\n" +
            "  `remark` STRING  COMMENT '最多可输入230个数字字母或115个汉字。',\n" +
            "  `drawer` STRING  COMMENT '开票人 最小4个字符最多10个汉字。',\n" +
            "  `payee` STRING  COMMENT '收款人 最小4个字符最多10个汉字。',\n" +
            "  `reviewer` STRING  COMMENT '复核人 最小4个字符最多10个汉字。',\n" +
            "  `invoice_type` INT  COMMENT '发票类型\\r\\n1：专用发票 2：电子专用发票 3：普通发票 4:电子普通发票\\r\\n对应静态数据表 business_name=output_invoice sub_business_name=invoice_type\\r\\n',\n" +
            "  `create_time` timestamp    COMMENT '创建时间',\n" +
            "  `update_time` timestamp    COMMENT '更新时间',\n" +
            "  `del_flag` INT   COMMENT '删除标志 0：未删除 1：已删除',\n" +
            "  `user_id` STRING  COMMENT '用户id',\n" +
            "  `source` INT  COMMENT '来源：1.微信小程序 2.支付宝小程序 0.其他',\n" +
            "  `yfpdm` STRING  COMMENT '原发票代码',\n" +
            "  `yfphm` STRING  COMMENT '原发票号码',\n" +
            "  `zhsl` STRING  COMMENT '综合税率 多税率情况保存为 -1 单税率情况直接存 0.13 小数最少两位 最多三位',\n" +
            "  `scbz` INT COMMENT '上传标志 0：未上传  1：已上传',\n" +
            "  `zfrq` timestamp  COMMENT '作废日期',\n" +
            "  `kprq` timestamp  COMMENT '开票日期(YYYY-MM-DD HH:MM:SS)',\n" +
            "  `zfr` STRING  COMMENT '作废人 最小4个字符最多10个汉字。',\n" +
            "  `tspz` STRING  COMMENT '特殊票种 暂只支持： 0 一般专普票 1-减按征收 11 差额征税',\n" +
            "  `tzdbh` STRING  COMMENT '通知单编号',\n" +
            "  `sellers_address_and_phone_number` STRING  COMMENT '销方地址电话',\n" +
            "  `special_invoice_type` INT  COMMENT '其他票种类型 0：正常专普票发票（默认）; 1：农产品销售发票;2：农产品收购发票（金税盘不支持）;8：成品油蓝字发票、成品油红字普通发票;18：成品油红字专用发票',\n" +
            "PRIMARY KEY (kp_output_invoice_id) NOT ENFORCED)" +
            "WITH ('connector' = 'mysql-cdc', " +
            "'hostname' = '47.99.140.202', " +
            "'port' = '33072', " +
            "'username' = 'root', " +
            "'password' = 'LjJl*ub#4*7^mJo', " +
            "'database-name' = 'ims', " +
            "'table-name' = 'kp_output_invoice'," +
            "'server-id'   = '5400-5404'," +
            "'server-time-zone'   = 'Asia/Shanghai'" +
            ")";

    static String sinkTable = "CREATE TABLE `sink_kp_output_invoice` (" +
            "invoice_date STRING," +
            "sellers_tax_name STRING," +
            "sellers_tax_number STRING," +
            "tax_amount DECIMAL(18,2)," +
            "total_price_tax DECIMAL(18,2)," +
            "price DECIMAL(18,2)," +
            "PRIMARY KEY (invoice_date,sellers_tax_name,sellers_tax_number) NOT ENFORCED)" +
            "WITH (" +
            "   'connector'  = 'jdbc'," +
            "   'url'        = 'jdbc:mysql://47.99.140.202:33072/ims'," +
            "   'table-name' = 'sink_kp_output_invoice'," +
            "   'driver'     = 'com.mysql.cj.jdbc.Driver'," +
            "   'username'   = 'root'," +
            "   'password'   = 'LjJl*ub#4*7^mJo'" +
            ")";


    static String sourceToSink = "INSERT INTO sink_kp_output_invoice " +
            "SELECT " +
            "invoice_date," +
            "sellers_tax_name," +
            "sellers_tax_number, " +
            "sum(tax_amount) as tax_amount ," +
            "sum(total_price_tax) as total_price_tax, " +
            "sum(price) as price " +
            "FROM source_kp_output_invoice " +
            "where invoice_date is not null and invoice_date <> '' " +
            "group by invoice_date,sellers_tax_name,sellers_tax_number";

    static String changeLog = "select * from source_kp_output_invoice";


//    @PostConstruct
    public void run() {
        Configuration configuration = new Configuration();
        configuration.setString(RestOptions.BIND_PORT, "8082");
//        configuration.setString(CheckpointingOptions.CHECKPOINT_STORAGE, "jobmanager");
//        configuration.setString(SavepointConfigOptions.SAVEPOINT_PATH, "/Users/chensong/Desktop/flink_savepoint/");
        configuration.setString(StateBackendOptions.STATE_BACKEND, "hashmap");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration)
                .enableCheckpointing(30000, CheckpointingMode.EXACTLY_ONCE)
                .setMaxParallelism(4)
                .setParallelism(4);
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);
        sqlModel(tEnv);
    }


    private static void sqlModel(TableEnvironment tEnv) {
        tEnv.executeSql(sourceTable);
        tEnv.executeSql(sinkTable);
        tEnv.executeSql(sourceToSink);
    }
}
