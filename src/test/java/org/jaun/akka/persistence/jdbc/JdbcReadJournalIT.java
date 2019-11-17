package org.jaun.akka.persistence.jdbc;

import akka.NotUsed;
import akka.actor.testkit.typed.javadsl.ActorTestKit;
import akka.actor.typed.javadsl.Adapter;
import akka.persistence.query.EventEnvelope;
import akka.persistence.query.PersistenceQuery;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import com.typesafe.config.ConfigFactory;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static com.google.common.truth.Truth.assertThat;

class JdbcReadJournalIT {

    private static ActorTestKit actorTestKit = ActorTestKit.create("test", ConfigFactory.defaultApplication());

    private static JdbcEventsDao dao;

    @BeforeAll
    private static void beforeAll() throws Exception {

        TestDb.setup();

        dao = new JdbcEventsDao();
    }

    @Test
    void currentEventsByPersistenceId() throws ExecutionException, InterruptedException {

        // prepare
        String stream = UUID.randomUUID().toString();

        PersistentEvent event = PersistentEventFixture.persistentEvent(stream).build();

        dao.write(Collections.singletonList(event));

        JdbcReadJournal readJournal = PersistenceQuery.get(Adapter.toClassic(actorTestKit.system())).getReadJournalFor(JdbcReadJournal.class,
                JdbcReadJournal.Identifier());

        // run
        Source<EventEnvelope, NotUsed> source = readJournal.currentEventsByPersistenceId(stream, 0, Long.MAX_VALUE);

        // verify
        List<EventEnvelope> eventEnvelopes = source.map(i -> i).runWith(Sink.seq(), actorTestKit.system()).toCompletableFuture().get();

        assertThat(eventEnvelopes).hasSize(1);
        assertThat(eventEnvelopes.get(0).persistenceId()).isEqualTo(stream);
        assertThat(eventEnvelopes.get(0).sequenceNr()).isEqualTo(1);

        MyPersistentBehavior.TestEvent testEvent = (MyPersistentBehavior.TestEvent) eventEnvelopes.get(0).event();

        assertThat(testEvent.getValue()).isEqualTo("do write test");
    }

}