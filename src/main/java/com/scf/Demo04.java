package com.scf;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 1.获取环境
 * 2.获取数据流
 * 3.调用算子处理
 * 4.输出
 */
public class Demo04 {
    public static void main(String[] args) {
        // 获取批处理环境
        ExecutionEnvironment ee = ExecutionEnvironment.getExecutionEnvironment();
        // 获取流处理环境
        StreamExecutionEnvironment piplineSee = StreamExecutionEnvironment.getExecutionEnvironment();
        // 学习测试时的一个本地 带WEB界面的环境
        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 8888); // 设置web ui界面的端口  http://localhost:8888
//        StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
    }
}
