package ac.cn.saya.flink.wc;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * 具有批处理的字符统计
 * @Title: WordCount
 * @ProjectName flink-util
 * @Description: TODO
 * @Author saya
 * @Date: 2022/1/28 13:46
 * @Description:
 */

public class BatchWordCount {

    public static void main(String[] args) throws Exception {
        // 1、创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        String inputPath = "/Users/saya/project/java/flink-util/src/main/resources/wc-sample.txt";
        // 2、从文件中读取数据（每一行）
        DataSet<String> dataSource = env.readTextFile(inputPath);
        // 3、将每行的数据进行分词，转换成二元组类型
        final FlatMapOperator<String, Tuple2<String, Integer>> wordAndOneTupe = dataSource.flatMap(new WordCountFlatMap()).returns(Types.TUPLE(Types.STRING, Types.INT));
        // 按照元组中第一个位置进行分组
        final UnsortedGrouping<Tuple2<String, Integer>> wordAndOneGroup = wordAndOneTupe.groupBy(0);
        // 分组内进行聚合统计
        DataSet<Tuple2<String, Integer>> dataResult = wordAndOneGroup.sum(1);
        dataResult.print();
    }

}
