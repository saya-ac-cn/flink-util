package ac.cn.saya.flink.udf;

import ac.cn.saya.flink.entity.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;
import scala.Tuple2;

/**
 * @Title: Test_TableFunction
 * @ProjectName flink-util
 * @Description: TODO
 * @Author saya
 * @Date: 2022/2/1 19:45
 * @Description:
 */

public class Test_TableFunction {

    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 从文件中读取
        DataStream<String> sourceStream = env.readTextFile("/Users/saya/project/java/flink-util/src/main/resources/sensor-sample.txt");

        // 将文本数据清洗成对象数据
        DataStream<SensorReading> dataStream = sourceStream.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String line) throws Exception {
                String[] fileds = line.split(",");
                return new SensorReading(fileds[0],Long.parseLong(fileds[1]),Double.parseDouble(fileds[2]));
            }
        });

        // 将流转换成表
        Table sensorTable = tableEnv.fromDataStream(dataStream, "id,timestamp as ts,temperature");

        Split split = new Split("_");
        // 需要在环境中注册UDF
        tableEnv.registerFunction("split",split);
        Table resultTable = sensorTable.joinLateral("split(id) as (word,length)").select("id,ts,word,length");


        tableEnv.createTemporaryView("sensor",sensorTable);
        Table resultSqlTable = tableEnv.sqlQuery("select id,ts,word,length from sensor,lateral table(split(id)) as splitid(word,length)");

        tableEnv.toAppendStream(resultTable, Row.class).print("orm");
        tableEnv.toAppendStream(resultSqlTable, Row.class).print("sql");

        env.execute();

    }

    // 实现自定义的scalarFunction，用元组要报错
    public static class Split extends TableFunction<Row> {

        private String separator = ";";

        public Split(String separator) {
            this.separator = separator;
        }

        //这个方法很重要，不写的话会报错！！！！
        //SQL validation failed. From line 1, column 89 to line 1, column 111: List of column aliases must have same degree as table; table has 1 columns ('f0'), whereas alias list has 3 columns
        //at org.apache.flink.table.planner.calcite.FlinkPlannerImpl.validate(FlinkPlannerImpl.scala:125)
        @Override
        public TypeInformation<Row> getResultType() {
            return Types.ROW(Types.STRING, Types.INT);
        }

        public void eval(String val){
            for (String item:val.split(separator)) {
                collect(Row.of(item,item.length()));
            }
        }
    }

}
