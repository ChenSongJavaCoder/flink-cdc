package com.example.finkcdc.realtime;

import org.apache.flink.configuration.*;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

/**
 * @author: CS
 * @date: 2022/7/13 下午3:20
 * @description:
 */
@Component
public class InvoiceStatistics {

    static String sourceTableIfsInvoice =
            "CREATE TABLE source_invoice_vat (\n" +
                    "id BIGINT NOT NULL COMMENT '主键id',\n" +
                    "zhqysh VARCHAR NOT NULL COMMENT '账户企业税号',\n" +
                    "qyfpfl INT NOT NULL COMMENT '企业发票分类（0-销项，1-进项）',\n" +
                    "fpdm VARCHAR NOT NULL COMMENT '发票代码',\n" +
                    "fphm VARCHAR NOT NULL COMMENT '发票号码',\n" +
                    "kprq TIMESTAMP NOT NULL COMMENT '开票日期',\n" +
                    "fplx INT COMMENT '发票类型（0-红字、1-蓝字）',\n" +
                    "fpzt INT COMMENT '发票状态',\n" +
                    "fpzldm VARCHAR NOT NULL COMMENT '发票种类',\n" +
                    "tspzbs VARCHAR NOT NULL COMMENT '特殊票种标识',\n" +
                    "ncpbz INT NOT NULL COMMENT '农产品标志（0-非农产品，1-农产品销售，2-农产品收购）',\n" +
                    "jym VARCHAR ( 100 ) COMMENT '校验码',\n" +
                    "mmq VARCHAR ( 200 ) COMMENT '密码区',\n" +
                    "bz VARCHAR ( 250 ) COMMENT '备注',\n" +
                    "xsfmc VARCHAR ( 100 ) COMMENT '销售方名称',\n" +
                    "xsfsh VARCHAR ( 100 ) NOT NULL COMMENT '销售方税号',\n" +
                    "xsfkhh VARCHAR ( 100 ) COMMENT '销售方开户行',\n" +
                    "xsfyhzh VARCHAR ( 100 ) COMMENT '销售方银行账号',\n" +
                    "xsfkhhzh VARCHAR ( 100 ) COMMENT '销售方开户行及账号',\n" +
                    "xsfdz VARCHAR ( 255 ) COMMENT '销售方地址',\n" +
                    "xsfdh VARCHAR ( 30 ) COMMENT '销售方电话',\n" +
                    "xsfdzdh VARCHAR ( 255 ) COMMENT '销售方地址电话',\n" +
                    "gmfmc VARCHAR ( 100 ) COMMENT '购买方名称',\n" +
                    "gmfsh VARCHAR ( 100 ) NOT NULL COMMENT '购买方税号',\n" +
                    "gmfkhh VARCHAR ( 100 ) COMMENT '购买方开户行',\n" +
                    "gmfyhzh VARCHAR ( 100 ) COMMENT '购买方银行账号',\n" +
                    "gmfkhhzh VARCHAR ( 100 ) COMMENT '购买方开户行及账号',\n" +
                    "gmfdz VARCHAR ( 255 ) COMMENT '购买方地址',\n" +
                    "gmfdh VARCHAR ( 30 ) COMMENT '购买方电话',\n" +
                    "gmfdzdh VARCHAR ( 255 ) COMMENT '购买方地址电话',\n" +
                    "jdhm VARCHAR ( 100 ) COMMENT '机打号码',\n" +
                    "jqbh VARCHAR ( 100 ) COMMENT '机器编号',\n" +
                    "hjje DECIMAL ( 18, 4 ) COMMENT '合计金额',\n" +
                    "hjse DECIMAL ( 18, 4 ) COMMENT '合计税额',\n" +
                    "jshj DECIMAL ( 18, 4 ) COMMENT '价税合计',\n" +
                    "kpr VARCHAR ( 100 ) COMMENT '开票人',\n" +
                    "skr VARCHAR ( 100 ) COMMENT '收款人',\n" +
                    "fhr VARCHAR ( 100 ) COMMENT '复核人',\n" +
                    "zfrq TIMESTAMP COMMENT '作废日期',\n" +
                    "zfr VARCHAR ( 100 ) COMMENT '作废人',\n" +
                    "yfpdm VARCHAR ( 50 ) NOT NULL COMMENT '原发票代码',\n" +
                    "yfphm VARCHAR ( 50 ) NOT NULL COMMENT '原发票号码',\n" +
                    "tzdbh VARCHAR ( 255 ) NOT NULL COMMENT '红字信息表通知单编号',\n" +
                    "ly INT COMMENT '来源',\n" +
                    "djzt INT COMMENT '单据状态',\n" +
                    "ssyf VARCHAR ( 8 ) NOT NULL COMMENT '所属月份',\n" +
                    "gxzt INT COMMENT '勾选状态（0-未勾选；1-已勾选）',\n" +
                    "gxsj TIMESTAMP COMMENT '勾选时间',\n" +
                    "gxrzsj TIMESTAMP COMMENT '勾选认证时间',\n" +
                    "gxyt INT COMMENT '勾选用途（进项特有）',\n" +
                    "rzzt INT COMMENT '认证状态: 0-未认证 1-已认证',\n" +
                    "bdklx INT COMMENT '不抵扣类型',\n" +
                    "dkbz INT COMMENT '代开标志（0-否，1-是）',\n" +
                    "dkfsh VARCHAR ( 100 ) NOT NULL COMMENT '代开方税号',\n" +
                    "dkfmc VARCHAR ( 100 ) COMMENT '代开方名称',\n" +
                    "PRIMARY KEY ( id ) NOT ENFORCED \n" +
                    ") WITH (\n" +
                    "'connector' = 'mysql-cdc',\n" +
                    "'hostname' = '47.99.140.202',\n" +
                    "'port' = '33071',\n" +
                    "'username' = 'root',\n" +
                    "'password' = 'LjJl*ub#4*7^mJo',\n" +
                    "'database-name' = 'ifs',\n" +
                    "'table-name' = 'invoice_vat_[0-9]+',\n" +
                    "'server-id' = '5400-5408',\n" +
                    "'server-time-zone' = 'Asia/Shanghai', \n" +
                    "'connect.timeout' = '60s', \n" +
                    "'debezium.wait_timeout' = '600000' \n" +
                    ")";

    static String sourceTableImsCustomer = "" +
            "CREATE TABLE `source_kp_custom` (\n" +
            "  `custom_id` varchar(20)  NOT NULL COMMENT '企业id',\n" +
            "  `group_id` varchar(20)   COMMENT '组织id',\n" +
            "  `custom_name` varchar(100)   COMMENT '企业名称',\n" +
            "  `plat_num` varchar(50)   COMMENT '税盘号',\n" +
            "  `custom_duty` varchar(50)   COMMENT '企业税号',\n" +
            "  `custom_type` varchar(2)   COMMENT '【新的企业类型，即特殊企业标识 01一般纳税人、08小规模纳税人、05转登记纳税人等）',\n" +
            "  `auto_status` int  COMMENT '授权状态 0未授权 1已授权',\n" +
            "  `create_time` timestamp  COMMENT '添加时间',\n" +
            "  `create_user` varchar(50)   COMMENT '添加人员',\n" +
            "  `update_time` timestamp  COMMENT '更新时间',\n" +
            "  `update_user` varchar(50)   COMMENT '更新人员',\n" +
            "  `del_flag` int  COMMENT '删除标志 0：未删除 1：已删除\\n删除标志 0：未删除 1：已删除\\n',\n" +
            "  `top_group_id` varchar(20)   COMMENT '顶级组织id',\n" +
            "  `top_group_name` varchar(255)   COMMENT '顶级组织名称',\n" +
            "  `opening_status` int  COMMENT '开通状态 1:已开通  2:未开通 3：已停用 4:已过期',\n" +
            "  `last_use_time` timestamp  COMMENT '最后一次使用时间',\n" +
            "  `db_update_time` timestamp NOT NULL COMMENT '统计使用更新时间',\n" +
            "  `company_id` varchar(20)  COMMENT '公司公共企业id',\n" +
            "  `source` varchar(20)  COMMENT '来源信息 10：伙伴系统',\n" +
            "  `canceled` int COMMENT '企业是否已注销',\n" +
            "  PRIMARY KEY (`custom_id`)  NOT ENFORCED\n" +
            ") WITH (\n" +
            "'connector' = 'mysql-cdc',\n" +
            "'hostname' = '47.99.140.202',\n" +
            "'port' = '33071',\n" +
            "'username' = 'root',\n" +
            "'password' = 'LjJl*ub#4*7^mJo',\n" +
            "'database-name' = 'ims',\n" +
            "'table-name' = 'kp_custom',\n" +
            "'server-id' = '5500-5508',\n" +
            "'server-time-zone' = 'Asia/Shanghai', \n" +
            "'connect.timeout' = '60s', \n" +
            "'debezium.wait_timeout' = '600000' \n" +
            ")";

    static String sinkTableIfsStatOutputInvoiceDaily = "CREATE TABLE `sink_stat_output_invoice_daily` (\n" +
            "  `company_name` varchar(200) NOT NULL COMMENT '企业名称',\n" +
            "  `company_tax_number` varchar(30) NOT NULL COMMENT '企业税号',\n" +
            "  `company_tax_nature` varchar(10) NOT NULL COMMENT '纳税人性质',\n" +
            "  `invoice_type` varchar(3) NOT NULL COMMENT '发票种类',\n" +
            "  `biz_date` varchar(10) NOT NULL COMMENT '统计日期 格式yyyy-MM-dd',\n" +
            "  `total_invoice_count` bigint NOT NULL COMMENT '总开票数量',\n" +
            "  `total_invoice_amount` DECIMAL(18,2)  COMMENT '总开票金额',\n" +
            "  `total_invoice_tax`DECIMAL(18,2)  COMMENT '总开票金额',\n" +
            "  `total_invoice_amount_tax` DECIMAL(18,2)  COMMENT '总开票价税合计',\n" +
            "  `blue_invoice_count` bigint NOT NULL COMMENT '蓝票开票数量',\n" +
            "  `blue_invoice_amount` DECIMAL(18,2)  COMMENT '蓝票开票金额',\n" +
            "  `blue_invoice_tax`DECIMAL(18,2)  COMMENT '蓝票开票金额',\n" +
            "  `blue_invoice_amount_tax` DECIMAL(18,2)  COMMENT '蓝票开票价税合计',\n" +
            "  `red_invoice_count` bigint NOT NULL COMMENT '红票开票数量',\n" +
            "  `red_invoice_amount` DECIMAL(18,2)  COMMENT '红票开票金额',\n" +
            "  `red_invoice_tax`DECIMAL(18,2)  COMMENT '红票开票金额',\n" +
            "  `red_invoice_amount_tax` DECIMAL(18,2)  COMMENT '废票开票价税合计',\n" +
            "  `invalid_invoice_count` bigint NOT NULL COMMENT '废票开票数量',\n" +
            "  `invalid_invoice_amount` DECIMAL(18,2)  COMMENT '废票开票金额',\n" +
            "  `invalid_invoice_tax`DECIMAL(18,2)  COMMENT '废票开票金额',\n" +
            "  `invalid_invoice_amount_tax` DECIMAL(18,2)  COMMENT '废票开票价税合计',\n" +
            "  `invalid_blue_invoice_count` bigint NOT NULL COMMENT '蓝废票开票数量',\n" +
            "  `invalid_blue_invoice_amount` DECIMAL(18,2)  COMMENT '蓝废票开票金额',\n" +
            "  `invalid_blue_invoice_tax`DECIMAL(18,2)  COMMENT '蓝废票开票金额',\n" +
            "  `invalid_blue_invoice_amount_tax` DECIMAL(18,2)  COMMENT '蓝废票开票价税合计',\n" +
            "  `invalid_red_invoice_count` bigint NOT NULL COMMENT '红废票开票数量',\n" +
            "  `invalid_red_invoice_amount` DECIMAL(18,2)  COMMENT '红废票开票金额',\n" +
            "  `invalid_red_invoice_tax`DECIMAL(18,2)  COMMENT '红废票开票金额',\n" +
            "  `invalid_red_invoice_amount_tax` DECIMAL(18,2)  COMMENT '红废票开票价税合计',\n" +
            "  PRIMARY KEY (company_name,company_tax_number,company_tax_nature,invoice_type,biz_date) NOT ENFORCED" +
            ") " +
            "WITH (" +
            "     'connector' = 'jdbc'," +
            "     'url'        = 'jdbc:mysql://47.99.140.202:33071/ifs'," +
            "     'table-name' = 'stat_output_invoice_daily'," +
            "     'driver'     = 'com.mysql.cj.jdbc.Driver'," +
            "     'username' = 'root'," +
            "     'password' = 'LjJl*ub#4*7^mJo'" +
            "     )";


    static String invoicePreView = "create TEMPORARY view invoice_pre_view as " +
            "SELECT " +
            "xsfmc as company_name," +
            "xsfsh as company_tax_number," +
            "fpzldm as invoice_type, " +
            "SUBSTR(CAST(kprq AS VARCHAR),1,10) as biz_date, " +
            "count(xsfsh) as total_invoice_count, " +
            "sum(case when fpzt in (0,1) then hjje else 0 end) as total_invoice_amount, " +
            "sum(case when fpzt in (0,1) then hjse else 0 end) as total_invoice_tax ," +
            "sum(case when fpzt in (0,1) then jshj else 0 end) as total_invoice_amount_tax, " +
            "sum(case when fpzt in (0) then 1 else 0 end) as blue_invoice_count, " +
            "sum(case when fpzt in (0) then hjje else 0 end) as blue_invoice_amount, " +
            "sum(case when fpzt in (0) then hjse else 0 end) as blue_invoice_tax, " +
            "sum(case when fpzt in (0) then jshj else 0 end) as blue_invoice_amount_tax, " +
            "sum(case when fpzt in (1) then 1 else 0 end) as red_invoice_count, " +
            "sum(case when fpzt in (1) then hjje else 0 end) as red_invoice_amount, " +
            "sum(case when fpzt in (1) then hjse else 0 end) as red_invoice_tax, " +
            "sum(case when fpzt in (1) then jshj else 0 end) as red_invoice_amount_tax, " +
            "sum(case when fpzt in (2,3,4,5) then 1 else 0 end) as invalid_invoice_count, " +
            "sum(case when fpzt in (2,3,4,5) then hjje else 0 end) as invalid_invoice_amount, " +
            "sum(case when fpzt in (2,3,4,5) then hjse else 0 end) as invalid_invoice_tax, " +
            "sum(case when fpzt in (2,3,4,5) then jshj else 0 end) as invalid_invoice_amount_tax, " +
            "sum(case when fpzt in (3) then 1 else 0 end) as invalid_blue_invoice_count, " +
            "sum(case when fpzt in (3) then hjje else 0 end) as invalid_blue_invoice_amount, " +
            "sum(case when fpzt in (3) then hjse else 0 end) as invalid_blue_invoice_tax, " +
            "sum(case when fpzt in (3) then jshj else 0 end) as invalid_blue_invoice_amount_tax, " +
            "sum(case when fpzt in (4) then 1 else 0 end) as invalid_red_invoice_count, " +
            "sum(case when fpzt in (4) then hjje else 0 end) as invalid_red_invoice_amount, " +
            "sum(case when fpzt in (4) then hjse else 0 end) as invalid_red_invoice_tax, " +
            "sum(case when fpzt in (4) then jshj else 0 end) as invalid_red_invoice_amount_tax " +
            "FROM source_invoice_vat  " +
            "where zhqysh = xsfsh  " +
            "group by xsfmc,xsfsh,fpzldm,SUBSTR(CAST(kprq AS VARCHAR),1,10) ";


    static String syncToSink = "INSERT INTO sink_stat_output_invoice_daily " +
            "select " +
            "company_name," +
            "company_tax_number," +
            "(case when t.custom_type = '01' then '一般纳税人' " +
            "when t.custom_type = '05' then '转登记纳税人' " +
            "when t.custom_type = '08' then '小规模纳税人' else '' end) as company_tax_nature, " +
            "invoice_type, " +
            "biz_date, " +
            "total_invoice_count, " +
            "total_invoice_amount, " +
            "total_invoice_tax, " +
            "total_invoice_amount_tax, " +
            "blue_invoice_count, " +
            "blue_invoice_amount, " +
            "blue_invoice_tax, " +
            "blue_invoice_amount_tax, " +
            "red_invoice_count, " +
            "red_invoice_amount, " +
            "red_invoice_tax, " +
            "red_invoice_amount_tax, " +
            "invalid_invoice_count, " +
            "invalid_invoice_amount, " +
            "invalid_invoice_tax, " +
            "invalid_invoice_amount_tax, " +
            "invalid_blue_invoice_count, " +
            "invalid_blue_invoice_amount, " +
            "invalid_blue_invoice_tax, " +
            "invalid_blue_invoice_amount_tax, " +
            "invalid_red_invoice_count, " +
            "invalid_red_invoice_amount, " +
            "invalid_red_invoice_tax, " +
            "invalid_red_invoice_amount_tax " +
            "from invoice_pre_view v " +
            "left join source_kp_custom t on v.company_tax_number = t.custom_duty " +
            "where t.del_flag = 0 "
            ;

    @PostConstruct
    public void run() {
        Configuration configuration = new Configuration();
        configuration.setString(RestOptions.BIND_PORT, "8082");
        configuration.setString(CheckpointingOptions.CHECKPOINT_STORAGE, "filesystem");
        configuration.setString(CheckpointingOptions.CHECKPOINTS_DIRECTORY, "file:///Users/chensong/flink/checkpoint");
        configuration.setString(CheckpointingOptions.SAVEPOINT_DIRECTORY, "file:///Users/chensong/flink/savepoint");
        configuration.set(TaskManagerOptions.MANAGED_MEMORY_SIZE, MemorySize.parse("1024m"));
        configuration.setBoolean(RestOptions.ENABLE_FLAMEGRAPH, true);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration)
                .enableCheckpointing(60000, CheckpointingMode.EXACTLY_ONCE)
                .setMaxParallelism(8)
                .setParallelism(8);
        env.setStateBackend(new EmbeddedRocksDBStateBackend());
        env.getCheckpointConfig().setForceUnalignedCheckpoints(true);

        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);
        sqlModel(tEnv);

    }


    private static void sqlModel(TableEnvironment tEnv) {
        tEnv.executeSql(sourceTableIfsInvoice);
        tEnv.executeSql(sourceTableImsCustomer);
        tEnv.executeSql(sinkTableIfsStatOutputInvoiceDaily);
        tEnv.executeSql(invoicePreView);
        tEnv.executeSql(syncToSink);
    }
}
