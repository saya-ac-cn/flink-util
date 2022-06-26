package ac.cn.saya.flink.table;

import ac.cn.saya.flink.entity.Event;
import ac.cn.saya.flink.source.ClickSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;


import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author saya
 * @title: SimpleTableExample
 * @projectName flink-util
 * @description: TODO
 * @date: 2022/6/26 20:19
 * @description:
 */

public class SimpleTableExample {

   public static void main(String[] args) throws Exception{
      // 创建执行环境
      StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
      env.setParallelism(1);
      env.getConfig().setAutoWatermarkInterval(1000);

      // 读取数据，得到DataStream
      SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
              // 乱序流的watermark生成
              .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                      .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                         @Override
                         public long extractTimestamp(Event element, long recordTimeStamp) {
                            return element.timestamp;
                         }
                      }));

      // 创建表执行环境
      StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);

      // 将DataStream转换成Table
      Table eventTable = tableEnvironment.fromDataStream(stream);

      // 基于sql进行转换
      Table resultTable1 = tableEnvironment.sqlQuery("select user ,url,`timestamp` from " + eventTable);

      // 基于table转换
      Table resultTable2 = eventTable.select($("user"), $("url"),$("timestamp")).where($("user").isEqual("Alice"));

      // 打印输出
      tableEnvironment.toDataStream(resultTable1).print("result1");
      tableEnvironment.toDataStream(resultTable2).print("result2");

      tableEnvironment.createTemporaryView("clickTable",eventTable);
      Table aggResult = tableEnvironment.sqlQuery("select user,count(url) as total from clickTable group by user");
      tableEnvironment.toChangelogStream(aggResult).print("agg");

      env.execute();
   }

}
