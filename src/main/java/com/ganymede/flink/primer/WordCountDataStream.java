package com.ganymede.flink.primer;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 使用 Flink DataStream 创建流式应用，并满足如下条件
 * 监控文件夹，interval 为1秒
 * 读取文本数据，进行 word count
 * 单词按逗号分隔
 * Window 的 slide 为5秒，size 为20秒
 * 结果写入 csv 文件内
 * 使用 Accumulator 与 Counter 对单词个数与行数进行计数，并对长度大于10的单词进行计数
 * 需提交作业代码，并对作业的 Flink UI 进行截图。截图中需展示
 * Accumulator 结果
 */
public class WordCountDataStream {

    public static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);

        // set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // get input data
        DataStream<String> text;
        text = env.readTextFile("D:\\data\\input");

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        final int windowSize = params.getInt("window", 10);
        final int slideSize = params.getInt("slide", 5);

        DataStream<Tuple2<String, Integer>> counts =
                // split up the lines in pairs (2-tuples) containing: (word,1)
                text.flatMap(new WordCountMap())
                        // create windows of windowSize records slided every slideSize records
                        .keyBy(0)
                        .countWindow(windowSize, slideSize)
                        // group by the tuple field "0" and sum up tuple field "1"
                        .sum(1);


        counts.writeAsText("D:\\data\\output");

        // execute program
        env.execute("WindowWordCount");
    }
}


class WordCountMap implements FlatMapFunction<String, Tuple2<String, Integer>> {

    @Override
    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
        String[] words = value.toLowerCase().split("\\W+");

        for (String word : words) {
            if (word.length() > 0) {
                out.collect(new Tuple2<>(word, 1));
            }
        }
    }
}
