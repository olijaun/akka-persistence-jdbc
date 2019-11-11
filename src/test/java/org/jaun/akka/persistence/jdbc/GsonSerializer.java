package org.jaun.akka.persistence.jdbc;

import akka.serialization.JSerializer;
import com.google.gson.Gson;

import java.nio.charset.StandardCharsets;

public class GsonSerializer extends JSerializer {

    private Gson gson = new Gson();

    public GsonSerializer() {
    }

    @Override
    public int identifier() {
        return 424242;
    }

    @Override
    public byte[] toBinary(Object obj) {
        if (obj instanceof Event) {
            return gson.toJson(obj).getBytes(StandardCharsets.UTF_8);
        } else {
            throw new IllegalArgumentException("Unknown type: " + obj);
        }
    }

    @Override
    public boolean includeManifest() {
        return false;
    }

    @Override
    public Object fromBinaryJava(byte[] bytes, Class<?> manifest) {
        return gson.fromJson(new String(bytes, StandardCharsets.UTF_8), manifest);
    }
}
