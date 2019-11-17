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

import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.google.common.truth.Truth.assertThat;
import static java.util.Arrays.asList;

class PersistenceIdSourceTest {
    private static ActorTestKit actorTestKit = ActorTestKit.create("test", ConfigFactory.defaultApplication());

    @BeforeAll
    private static void beforeAll() throws Exception {
        TestDb.setup();
    }

    @Test
    void create() {

        JdbcEventsDao dao = new JdbcEventsDao();
        PersistentEvent event1 = PersistentEventFixture.persistentEvent("test1").sequenceNumber(1L).build();
        dao.write(asList(event1));

        Source<Set<String>, NotUsed> source = PersistenceIdSource.create(new JdbcEventsDao(), FiniteDuration.create(2000, TimeUnit.MILLISECONDS));

        Sink<Set<String>, TestSubscriber.Probe<Set<String>>> testSink = TestSink.probe(Adapter.toClassic(actorTestKit.system()));
        TestSubscriber.Probe<Set<String>> testProbe = source.runWith(testSink, actorTestKit.system());

        assertThat(testProbe.requestNext()).containsExactly(event1.getStream());

        PersistentEvent event2 = PersistentEventFixture.persistentEvent("test2").sequenceNumber(1L).build();
        dao.write(asList(event2));

        assertThat(testProbe.requestNext()).containsExactly(event2.getStream());

        testProbe.expectNoMessage();
    }
}