package ac.cn.saya.flink.table;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author saya
 * @title: TimeAndWIndowTest
 * @projectName flink-util
 * @description: TODO
 * @date: 2022/6/28 23:28
 * @description:
 */

public class TimeAndWIndowTest {

    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().setAutoWatermarkInterval(1000);

        // 创建表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 在创建表的DDL中直接定义时间属性
        String createDDL = "CREATE TABLE clickTable(username STRING,url STRING,ts BIGINT,et AS TO_TIMESTAMP(FROM_UNIXTIME(ts/1000)),WATERMARK FOR et AS et - INTERVAL '1' SECOND) " +
                "WITH ('connector'='filesystem','path'='/Users/saya/project/java/flink-util/src/main/resources/clickTable-1.csv','format'='csv')";

        TableResult clickTable = tableEnv.executeSql(createDDL);

        // 分组查询
        Table aggTable = tableEnv.sqlQuery("select username,count(1) from clickTable group by username");

        tableEnv.toChangelogStream(aggTable).print("group");

        env.execute();
    }

}
