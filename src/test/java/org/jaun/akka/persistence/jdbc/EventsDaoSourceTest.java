package org.jaun.akka.persistence.jdbc;

import akka.NotUsed;
import akka.actor.testkit.typed.javadsl.ActorTestKit;
import akka.actor.typed.javadsl.Adapter;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.stream.testkit.TestSubscriber;
import akka.stream.testkit.javadsl.TestSink;
import com.typesafe.config.ConfigFactory;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import scala.concurrent.duration.FiniteDuration;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.google.common.truth.Truth.assertThat;
import static java.util.Arrays.asList;

class EventsDaoSourceTest {

    private static ActorTestKit actorTestKit = ActorTestKit.create("test", ConfigFactory.defaultApplication());

    @BeforeAll
    private static void beforeAll() throws Exception {
        TestDb.setup();
    }

    @Test
    void create() {

        JdbcEventsDao dao = new JdbcEventsDao();
        PersistentEvent event1 = PersistentEventFixture.persistentEvent("test").sequenceNumber(1L).build();
        PersistentEvent event2 = PersistentEventFixture.persistentEvent("test").sequenceNumber(2L).build();
        dao.write(asList(event1, event2));

        Source<List<PersistentEvent>, NotUsed> source = EventsDaoSource.create(new JdbcEventsDao(), "test", 0, FiniteDuration.create(2000, TimeUnit.MILLISECONDS));

        ArrayList<PersistentEvent> readEvents = new ArrayList<>();

        source.runWith(Sink.foreach(p -> readEvents.addAll(p)), actorTestKit.system()).toCompletableFuture();

        Sink<List<PersistentEvent>, TestSubscriber.Probe<List<PersistentEvent>>> testSink = TestSink.probe(Adapter.toClassic(actorTestKit.system()));
        TestSubscriber.Probe<List<PersistentEvent>> testProbe = source.runWith(testSink, actorTestKit.system());

        assertThat(testProbe.requestNext()).containsExactly(event1, event2);

        PersistentEvent event3 = PersistentEventFixture.persistentEvent("test").sequenceNumber(3L).build();
        PersistentEvent event4 = PersistentEventFixture.persistentEvent("test").sequenceNumber(4L).build();
        dao.write(asList(event3, event4));

        assertThat(testProbe.requestNext()).containsExactly(event3, event4);

        testProbe.expectNoMessage();
    }
}