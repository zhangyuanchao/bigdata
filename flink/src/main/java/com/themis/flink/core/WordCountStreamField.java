package com.themis.flink.core;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * flink流处理按指定字段进行wordcount
 */
public class WordCountStreamField {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String host = parameterTool.get("host");
        int port = parameterTool.getInt("port");
        DataStream<String> text = env.socketTextStream(host, port);
        DataStream<WordCountEntity> words = text.flatMap(new FlatMapFunction<String, WordCountEntity>() {
            @Override
            public void flatMap(String line, Collector<WordCountEntity> collector) throws Exception {
                String[] words = line.split(" ");
                for (String word : words) {
                    collector.collect(new WordCountEntity(word, 1));
                }
            }
        });
        DataStream<WordCountEntity> out = words.keyBy(new KeySelector<WordCountEntity, String>() {

            @Override
            public String getKey(WordCountEntity wordCountEntity) throws Exception {
                return wordCountEntity.getWord();
            }
        }).sum("count");
        out.print();
        env.execute();

    }

    public static class WordCountEntity {
        private String word;
        private int count;

        public WordCountEntity() {

        }

        public WordCountEntity(String word, int count) {
            this.word = word;
            this.count = count;
        }

        public String getWord() {
            return word;
        }

        public void setWord(String word) {
            this.word = word;
        }

        public int getCount() {
            return count;
        }

        public void setCount(int count) {
            this.count = count;
        }

        @Override
        public String toString() {
            return "WordWithCount{" +
                    "word='" + word + '\'' +
                    ", count=" + count +
                    '}';
        }
    }
}
