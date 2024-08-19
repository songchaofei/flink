package com.scf.inc;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


public class Demo02 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置处理数据的模式  如果不设置的话 默认自动推断
//        see.getConfig().setExecutionMode(ExecutionMode.BATCH);  // 批处理
//        see.getConfig().setExecutionMode(ExecutionMode.PIPELINED);  // 流处理

        // 2 获取数据源
        // 获取网络流
        DataStreamSource<String> dss = see.socketTextStream("localhost", 8899);
        dss.print();
        see.execute();
    }
}
