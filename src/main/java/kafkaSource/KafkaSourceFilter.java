package kafkaSource;

import com.alibaba.fastjson.JSON;
import dto.Metric;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

import java.util.Properties;

public class KafkaSourceFilter {
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
                .filter(new FilterFunction<String>() {
                    @Override
                    public boolean filter(String str) throws Exception {
                        JSON json = JSON.parseObject(str);
                        Metric metric = JSON.toJavaObject(json, Metric.class);
                        if (metric.getValue() > 7)
                            return true;
                        else return false;
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
