package org.jaun.akka.persistence.jdbc;

import akka.NotUsed;
import akka.actor.testkit.typed.javadsl.ActorTestKit;
import akka.actor.typed.javadsl.Adapter;
import akka.persistence.query.EventEnvelope;
import akka.persistence.query.PersistenceQuery;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import com.google.common.truth.Truth;
import com.typesafe.config.ConfigFactory;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

class JdbcReadJournalIT {

    private static ActorTestKit actorTestKit = ActorTestKit.create("test", ConfigFactory.defaultApplication());

    private static JdbcEventsDao dao;

    @BeforeAll
    private static void beforeAll() throws Exception {
        dao = new JdbcEventsDao();

        InputStream inputStream = JdbcAsyncWriteJournal.class.getResourceAsStream("/eventstore.ddl");

        String ddl = new BufferedReader(new InputStreamReader(inputStream))
                .lines().collect(Collectors.joining("\n"));

        try ( //
              Connection conn = DriverManager.getConnection(JdbcEventsDao.DB_URL, null, null); //
              Statement stmt = conn.createStatement()) {

            conn.setAutoCommit(true);

            stmt.executeUpdate(ddl);
        }
    }

    void insert(PersistentEvent event) {
        dao.write(Collections.singletonList(event));
    }

    @Test
    void currentEventsByPersistenceId() throws ExecutionException, InterruptedException {

        // prepare
        String stream = UUID.randomUUID().toString();

        PersistentEvent event = PersistentEvent.builder()
                .stream(stream)
                .serializedEvent("{\"value\":\"do write test\"}".getBytes(StandardCharsets.UTF_8))
                .eventType("org.jaun.akka.persistence.jdbc.MyPersistentBehavior$TestEvent")
                .metadata(Collections.emptyMap())
                .sequenceNumber(1L)
                .tags(Collections.emptySet())
                .deleted(false).build();

        insert(event);

        JdbcReadJournal readJournal = PersistenceQuery.get(Adapter.toClassic(actorTestKit.system())).getReadJournalFor(JdbcReadJournal.class,
                JdbcReadJournal.Identifier());

        // run
        Source<EventEnvelope, NotUsed> source = readJournal.currentEventsByPersistenceId(stream, 0, Long.MAX_VALUE);

        // verify
        List<EventEnvelope> eventEnvelopes = source.map(i -> i).runWith(Sink.seq(), actorTestKit.system()).toCompletableFuture().get();

        assertThat(eventEnvelopes).hasSize(1);
        assertThat(eventEnvelopes.get(0).persistenceId()).isEqualTo(stream);
        assertThat(eventEnvelopes.get(0).sequenceNr()).isEqualTo(1);

        MyPersistentBehavior.TestEvent testEvent = (MyPersistentBehavior.TestEvent)eventEnvelopes.get(0).event();

        assertThat(testEvent.getValue()).isEqualTo("do write test");
    }

}