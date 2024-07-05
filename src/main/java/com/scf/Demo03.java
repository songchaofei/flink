package com.scf;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 使用lambda表达式编写wordcount单词统计代码
 * 如果使用lambda表达式实现抽线方法需要注意:
 * 1. 接口种只有一个抽象方法时才可以使用lambda表达式
 * 2. 注意返回值的类型  最好使用returns方法来明确返回值的类型
 */
public class Demo03 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> dataStream = see.socketTextStream("localhost", 9000);

//        SingleOutputStreamOperator<String> res = dataStream.map(line -> line.toUpperCase());
//        SingleOutputStreamOperator<String> res = dataStream.map(String::toUpperCase);
/*
        SingleOutputStreamOperator<String> res = dataStream.flatMap((String line, Collector<String> out) -> {
            String[] words = line.split("\\s+");
            for (String word : words) {
                out.collect(word);  // 底层在编译的时候 word的String类型会被擦除 需要调用returns方法来进行指定 以下列出四种指定方式
            }
        }).returns(TypeInformation.of(new TypeHint<String>() {})); // 通过类型信息的方式指定返回值类型  (比较通用)
        //.returns(TypeInformation.of(String.class));  // 通过类型信息的方式指定返回值类型  (比较通用)
        //.returns(String.class);  // 手动指定返回值的类型
        // .returns(new TypeHint<String>() {});  //  自动进行类型推断
*/
        // 使用元组的方式传递参数
        SingleOutputStreamOperator<Tuple2<String, Integer>> res = dataStream.flatMap((String line, Collector<Tuple2<String, Integer>> out) -> {
            String[] words = line.split("\\s+");
            for (String word : words) {
                Tuple2<String, Integer> tp2 = Tuple2.of(word, 1);
                out.collect(tp2);
            }
        }).returns(TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {
        }));

        SingleOutputStreamOperator<Tuple2<String, Integer>> res2 = res.keyBy(tp -> tp.f0).sum(1);
        System.out.println("并行度为:" + see.getParallelism());
        res2.print();
        see.execute();
    }
}
