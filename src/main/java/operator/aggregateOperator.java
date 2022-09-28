package operator;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import dto.Metric;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

import java.time.Duration;
import java.util.Properties;

public class aggregateOperator {

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
                .aggregate(new AggregateFunction<Metric, Tuple2<Long, Long>, Tuple2<Long, Long>>() {
                    @Override
                    public Tuple2<Long, Long> createAccumulator() {
                        return new Tuple2<>(0L, 0L);
                    }

                    @Override
                    public Tuple2<Long, Long> add(Metric value, Tuple2<Long, Long> accumulator) {
                        return new Tuple2<>(value.getValue() + accumulator.f0, accumulator.f1 + 1L);
                    }

                    @Override
                    public Tuple2<Long, Long> getResult(Tuple2<Long, Long> accumulator) {
                        return new Tuple2<Long, Long>(accumulator.f0, accumulator.f1);
                    }

                    @Override
                    public Tuple2<Long, Long> merge(Tuple2<Long, Long> a, Tuple2<Long, Long> b) {
                        return new Tuple2<Long, Long>(a.f0 + b.f0, a.f1 + b.f1);
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
