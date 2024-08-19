package com.scf.sink;

import com.alibaba.fastjson.JSON;
import com.scf.beans.UserBean;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.BasePathBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import static com.alibaba.fastjson.JSON.toJSON;
import static com.alibaba.fastjson.JSON.toJSONString;

public class Demo01SinkPrint {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 8888);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
//        env.setParallelism(3);
        env.enableCheckpointing(1000L);  // 开启checkpoint机制

        DataStreamSource<String> ds1 = env.socketTextStream("localhost", 8899);  // 并行度1
        DataStream<UserBean> ds2 = ds1.map(s -> {
            String[] arrs = s.split(",");
            return new UserBean(Integer.parseInt(arrs[0]), arrs[1], Integer.parseInt(arrs[2]), arrs[3], Long.parseLong(arrs[4]));
        });

        DataStream<Object> ds3 = ds2.map(s -> toJSONString(s));

        // 输出数据结果到File文件中，远远不断的写入
        Path path = new Path("data/sink");
        SimpleStringEncoder simpleStringEncoder = new SimpleStringEncoder();
        OutputFileConfig outputFileConfig = OutputFileConfig.builder()
                .withPartPrefix("dom-")
                .withPartSuffix(".txt")
                .build();
        FileSink build = FileSink.forRowFormat(path, simpleStringEncoder)
                .withBucketAssigner(new DateTimeBucketAssigner())
                .withRollingPolicy(DefaultRollingPolicy.builder()
                        .withInactivityInterval(200L)
                        .withMaxPartSize(1024)
                        .withRolloverInterval(10000L)
                        .build())
                .withOutputFileConfig(outputFileConfig)
                .build();

        env.execute("sink print");
    }
}
