package org.jaun.akka.persistence.jdbc;

import org.junit.jupiter.api.*;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import static com.google.common.truth.Truth.assertThat;
import static java.util.Arrays.asList;

class JdbcEventsDaoIT {

    @BeforeEach
    private void beforeEach() {
        TestDb.setup();
    }

    @AfterEach
    private void afterEach() throws Exception {
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

    @Test
    void readByTag() {

        JdbcEventsDao dao = new JdbcEventsDao();
        PersistentEvent event11 = PersistentEventFixture.persistentEvent("stream1").tags(setOf("tag1", "tag2")).sequenceNumber(1L).build();
        PersistentEvent event12 = PersistentEventFixture.persistentEvent("stream1").tags(setOf("tag2")).sequenceNumber(2L).build();
        PersistentEvent event21 = PersistentEventFixture.persistentEvent("stream2").tags(setOf("tag1", "tag3")).sequenceNumber(1L).build();
        PersistentEvent event22 = PersistentEventFixture.persistentEvent("stream2").tags(setOf("tag2")).sequenceNumber(2L).build();

        dao.write(asList(event11, event12, event21, event22));

        Iterable<PersistentEventWithOffset> iterable = dao.readByTag("tag1", 0);

        Iterator<PersistentEventWithOffset> iterator = iterable.iterator();

        assertThat(iterator.next()).isEqualTo(new PersistentEventWithOffset(event11, 1));
        assertThat(iterator.next()).isEqualTo(new PersistentEventWithOffset(event21, 3));

        assertThat(iterator.hasNext()).isFalse();
    }

    private Set<String> setOf(String... s) {
        return new HashSet<>(Arrays.asList(s));
    }
}