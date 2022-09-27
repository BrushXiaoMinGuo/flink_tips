package kafkaSource;

import com.alibaba.fastjson.JSON;
import dto.Metric;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

import java.util.Properties;

public class KafkaSourceMap {
    public static final String broker_list = "localhost:9092";
    public static final String topic = "test";

    public static void sourceKafka() throws Exception {
        // set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties props = new Properties();
        props.put("bootstrap.servers", broker_list);
        props.put("zookeeper.connect", "localhost:2181");
        props.put("group.id", "metric-group");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "latest");

        env.addSource(new FlinkKafkaConsumer("test", new SimpleStringSchema(), props))
                .map(new MapFunction<String, String>() {
                    @Override
                    public String map(String str) throws Exception {
                        JSON json = JSON.parseObject(str);
                        Metric metric = JSON.toJavaObject(json, Metric.class);
                        return metric.getName();
                    }
                })
                .print();

        env.execute("KafkaWordCount");

    }

    public static void main(String args[]) throws Exception {
        System.out.println("ok");
        sourceKafka();


    }
}
