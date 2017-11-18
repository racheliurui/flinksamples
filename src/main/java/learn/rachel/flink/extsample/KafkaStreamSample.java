package learn.rachel.flink.extsample;

import learn.rachel.flink.learn.rachel.flink.schema.MySimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

import java.util.Properties;

/**
 * Created by Rachel on 11/18/2017.
 * Link to java doc
 * https://www.javadoc.io/doc/org.apache.flink/flink-connector-kafka_2.11/0.10.2
 */
public class KafkaStreamSample {


    /*
    * Treat Kafka Topic as inbound streaming
    *
    * */
    public static DataStream<String> getKafkaStream(StreamExecutionEnvironment env, Properties kafkaConnProps, String topicName) {

        FlinkKafkaConsumer010 consumer = new FlinkKafkaConsumer010<>(
                topicName
                , new SimpleStringSchema(), kafkaConnProps
        );

        DataStream<String> stream_kafkaTopic = env
                .addSource(consumer);

        return stream_kafkaTopic;
    }


    public static void sinkToKafkaTopic(DataStream<String> stream, String bootstrapServers, String sinkTopicName) {

        FlinkKafkaProducer010<String> kafka_producer = new FlinkKafkaProducer010<String>(
                sinkTopicName,
                new MySimpleStringSchema(),
                getKafkaSinkConnProps(bootstrapServers)
        );

        // the following is necessary for at-least-once delivery guarantee
        // "false" by default
        kafka_producer.setLogFailuresOnly(false);

        stream.addSink(kafka_producer);
    }

    public static Properties getKafkaSinkConnProps(String bootstrapServers) {

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", bootstrapServers);

        // only required for Kafka 0.8
        //properties.setProperty("zookeeper.connect", "localhost:2181");
        //   properties.setProperty("group.id", groupId);

        return properties;
    }

    public static Properties getKafkaConnProps(String bootstrapServers, String groupId) {

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", bootstrapServers);

        // only required for Kafka 0.8
        //properties.setProperty("zookeeper.connect", "localhost:2181");
        properties.setProperty("group.id", groupId);

        return properties;
    }
}
