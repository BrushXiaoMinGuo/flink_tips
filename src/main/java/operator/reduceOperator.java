package operator;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import dto.Metric;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

import java.time.Duration;
import java.util.Properties;

public class reduceOperator {
    public static final String broker_list = "localhost:9092";
    public static final String topic = "test";

    public static void go() throws Exception {
        // set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties props = new Properties();
        props.put("bootstrap.servers", broker_list);
        props.put("zookeeper.connect", "localhost:2181");
        props.put("group.id", "metric-group");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "latest");

        DataStream<Metric> dataStream = env.addSource(new FlinkKafkaConsumer("test", new SimpleStringSchema(), props))
                .map(json -> JSON.toJavaObject(JSON.parseObject((String) json), Metric.class)).returns(Metric.class);

        dataStream
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Metric>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                                .withTimestampAssigner((event, timestamp) -> event.getTimestamp())
                )
                .keyBy(event -> event.getName())

                .window(TumblingEventTimeWindows.of(Time.seconds(5)))

                .reduce(new ReduceFunction<Metric>() {
                    @Override
                    public Metric reduce(Metric value1, Metric value2) throws Exception {
                        return new Metric(value1.getName(), value2.getTimestamp(), value1.getValue() + value2.getValue());
                    }
                })
                .map(event -> JSONObject.toJSON(event))
                .print();
        env.execute("KafkaWordCount");

    }

    public static void main(String args[]) throws Exception {
        System.out.println("ok");
        go();


    }
}
