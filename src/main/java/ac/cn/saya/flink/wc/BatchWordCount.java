package ac.cn.saya.flink.wc;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
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
        // 创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        String inputPath = "/Users/saya/project/java/flink-util/src/main/resources/wc-sample.txt";
        // 从文件中读取数据
        DataSet<String> dataSource = env.readTextFile(inputPath);
        DataSet<Tuple2<String, Integer>> dataResult = dataSource.flatMap(new WordCountFlatMap())
                // 按照元组中第一个位置进行分组
                .groupBy(0)
                // 把元组中第2个位置的数做相加操作
                .sum(1);
        dataResult.print();
    }

}
