package learn.rachel.flink.extsample;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

/**
 * Created by Rachel on 11/18/2017.
 */
public class KafkaStreamSampleTest {
    static String bootstrapServers = "192.168.1.222:9092";
    static Properties kafkaConnProps = KafkaStreamSample.getKafkaConnProps(bootstrapServers, "testes");
    static final StreamExecutionEnvironment env = StreamExecutionEnvironment
            .getExecutionEnvironment();
    static String inboundTopic = "TestIn";
    static String outboundTopic = "TestOut";

    //https://ci.apache.org/projects/flink/flink-docs-release-1.3/dev/datastream_api.html#example-program
    public static void main(String[] args) throws Exception {
        //setup env
        s1(env, kafkaConnProps, inboundTopic, bootstrapServers, outboundTopic);


    }


    /* Treat topic string as stream ; sink to another topic without any transformation*/
    private static void s1(StreamExecutionEnvironment env, Properties inboundKafkaConsumerProps, String topicIn, String outboundKafkaSinkBootStrapServers, String topicOut) throws Exception {
        DataStream<String> stream_kafkaTopic = KafkaStreamSample.getKafkaStream(env, inboundKafkaConsumerProps, topicIn);

        KafkaStreamSample.sinkToKafkaTopic(stream_kafkaTopic, outboundKafkaSinkBootStrapServers, topicOut);

        env.execute("Simply read steam from " + topicIn + " and sink to " + topicOut);

    }


}
