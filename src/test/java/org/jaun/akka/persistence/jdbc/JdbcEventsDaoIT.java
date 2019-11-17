package org.jaun.akka.persistence.jdbc;

import com.google.common.truth.Truth;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;

import static com.google.common.truth.Truth.assertThat;
import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.*;

class JdbcEventsDaoIT {

    @BeforeAll
    private static void beforeAll() throws Exception {
        TestDb.setup();
    }

    @AfterAll
    private static void afterAll() throws Exception {
        TestDb.printTables();
    }

    @Test
    void persistenceIds() {

        JdbcEventsDao dao = new JdbcEventsDao();
        PersistentEvent event11 = PersistentEventFixture.persistentEvent("stream1").sequenceNumber(1L).build();
        PersistentEvent event12 = PersistentEventFixture.persistentEvent("stream1").sequenceNumber(2L).build();
        PersistentEvent event21 = PersistentEventFixture.persistentEvent("stream2").sequenceNumber(1L).build();
        PersistentEvent event22 = PersistentEventFixture.persistentEvent("stream2").sequenceNumber(2L).build();

        dao.write(asList(event11, event12, event21, event22));

        Iterable<String> iterable = dao.persistenceIds();

        Iterator<String> iterator = iterable.iterator();

        assertThat(iterator.next()).isEqualTo("stream1");
        assertThat(iterator.next()).isEqualTo("stream2");

        assertThat(iterator.hasNext()).isFalse();
    }
}