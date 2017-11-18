package learn.rachel.flink.learn.rachel.flink.schema;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;
import org.apache.flink.streaming.util.serialization.SerializationSchema;

import java.io.IOException;

/**
 * Created by Rachel on 11/18/2017.
 */
public class MySimpleStringSchema implements DeserializationSchema<String>,
        SerializationSchema<String> {


    @Override
    public String deserialize(byte[] bytes) throws IOException {
        return new String(bytes);
    }

    @Override
    public boolean isEndOfStream(String s) {
        return false;
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return TypeExtractor.getForClass(String.class);
    }

    @Override
    public byte[] serialize(String s) {
        return s.getBytes();
    }
}
