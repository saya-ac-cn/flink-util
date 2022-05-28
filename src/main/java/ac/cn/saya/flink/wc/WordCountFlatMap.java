package ac.cn.saya.flink.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @Title: WordCountFlatMap
 * @ProjectName flink-util
 * @Description: TODO
 * @Author saya
 * @Date: 2022/1/28 14:04
 * @Description:
 */

public class WordCountFlatMap implements FlatMapFunction<String, Tuple2<String,Integer>> {

    /**
     *
     * @param line 每行数据
     * @param collector 处理后的收集器
     * @throws Exception
     */
    @Override
    public void flatMap(String line, Collector<Tuple2<String, Integer>> collector) throws Exception {
        // 按照空格进行分词
        String[] words = line.split(" ");
        // 遍历所有的word，封装成一个二元组
        for (String word: words){
            collector.collect(new Tuple2(word,1));
        }
    }
}
