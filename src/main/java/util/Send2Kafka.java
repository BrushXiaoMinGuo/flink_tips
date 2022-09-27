package util;

import java.util.Properties;

import com.alibaba.fastjson.JSON;
import dto.Metric;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class Send2Kafka {

    public static final String broker_list = "localhost:9092";
    public static final String topic = "test";

    public static void write2Kafka() throws InterruptedException {

        Properties props = new Properties();
        props.put("bootstrap.servers", broker_list);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        Producer<String, String> producer = new KafkaProducer<>(props);
        while (true){
            Metric metric = new Metric("pv", 1);
            producer.send( new ProducerRecord(topic, JSON.toJSONString(metric)));
            Thread.sleep(5000);

        }

    }

    public static void main(String args[]) throws InterruptedException {
        System.out.println("ok");
        write2Kafka();


    }
}
