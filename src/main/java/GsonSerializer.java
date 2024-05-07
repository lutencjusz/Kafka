import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class GsonSerializer<T> implements Serializer<T> {

    Gson gson = new GsonBuilder().create();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        Serializer.super.configure(configs, isKey);
    }

    @Override
    public byte[] serialize(String s, T t) {
        return gson.toJson(t).getBytes();
    }

    @Override
    public byte[] serialize(String topic, Headers headers, T data) {
        return gson.toJson(data).getBytes();
    }

    @Override
    public void close() {
        Serializer.super.close();
    }
}
