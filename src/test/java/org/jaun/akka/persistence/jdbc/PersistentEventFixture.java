package org.jaun.akka.persistence.jdbc;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;

public class PersistentEventFixture {

    public static PersistentEvent.Builder persistentEvent(String stream) {

        HashMap<String, String> metadata = new HashMap<>();
        metadata.put(Converter.AKKA_SERIALIZER_ID, String.valueOf(new GsonSerializer().identifier()));

        return PersistentEvent.builder()
                .stream(stream)
                .serializedEvent("{\"value\":\"do write test\"}".getBytes(StandardCharsets.UTF_8))
                .eventType(MyPersistentBehavior.TestEvent.class.getName())
                .metadata(metadata)
                .sequenceNumber(1L)
                .tags(Collections.emptySet())
                .deleted(false);
    }
}
