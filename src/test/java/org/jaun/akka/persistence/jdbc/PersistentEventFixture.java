package org.jaun.akka.persistence.jdbc;

import java.nio.charset.StandardCharsets;
import java.util.Collections;

public class PersistentEventFixture {

    public static PersistentEvent.Builder persistentEvent(String stream) {
        return PersistentEvent.builder()
                .stream(stream)
                .serializedEvent("{\"value\":\"do write test\"}".getBytes(StandardCharsets.UTF_8))
                .eventType("org.jaun.akka.persistence.jdbc.MyPersistentBehavior$TestEvent")
                .metadata(Collections.emptyMap())
                .sequenceNumber(1L)
                .tags(Collections.emptySet())
                .deleted(false);
    }
}
