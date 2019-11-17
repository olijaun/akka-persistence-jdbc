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

import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.google.common.truth.Truth.assertThat;
import static java.util.Arrays.asList;

class PersistentEventByStreamSourceTest {

    private static ActorTestKit actorTestKit = ActorTestKit.create("test", ConfigFactory.defaultApplication());

    @BeforeAll
    private static void beforeAll() throws Exception {
        TestDb.setup();
    }

    @Test
    void create() {

        JdbcEventsDao dao = new JdbcEventsDao();
        PersistentEventWithOffset event1 = new PersistentEventWithOffset(PersistentEventFixture.persistentEvent("test").sequenceNumber(1L).build(), 1L);
        PersistentEventWithOffset event2 = new PersistentEventWithOffset(PersistentEventFixture.persistentEvent("test").sequenceNumber(2L).build(), 2L);
        dao.write(asList(event1.getEvent(), event2.getEvent()));

        Source<List<PersistentEventWithOffset>, NotUsed> source = PersistentEventByStreamSource.create(new JdbcEventsDao(), "test", 0, FiniteDuration.create(2000, TimeUnit.MILLISECONDS));

        Sink<List<PersistentEventWithOffset>, TestSubscriber.Probe<List<PersistentEventWithOffset>>> testSink = TestSink.probe(Adapter.toClassic(actorTestKit.system()));
        TestSubscriber.Probe<List<PersistentEventWithOffset>> testProbe = source.runWith(testSink, actorTestKit.system());

        assertThat(testProbe.requestNext()).containsExactly(event1, event2);

        PersistentEventWithOffset event3 = new PersistentEventWithOffset(PersistentEventFixture.persistentEvent("test").sequenceNumber(3L).build(), 3L);
        PersistentEventWithOffset event4 = new PersistentEventWithOffset(PersistentEventFixture.persistentEvent("test").sequenceNumber(4L).build(), 4L);
        dao.write(asList(event3.getEvent(), event4.getEvent()));

        assertThat(testProbe.requestNext()).containsExactly(event3, event4);

        testProbe.expectNoMessage();
    }
}