package org.jaun.akka.persistence.jdbc;

import akka.NotUsed;
import akka.actor.testkit.typed.javadsl.ActorTestKit;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import com.typesafe.config.ConfigFactory;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import scala.concurrent.duration.FiniteDuration;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.*;

class EventsDaoSourceTest {

    private static ActorTestKit actorTestKit = ActorTestKit.create("test", ConfigFactory.defaultApplication());

    @BeforeAll
    private static void beforeAll() throws Exception {
        DbSetup.setup();
    }

    @Test
    void create() throws Exception {

        DbSetup.getDataSource();

        JdbcEventsDao dao = new JdbcEventsDao();
        PersistentEvent event1 = PersistentEventFixture.persistentEvent("test").sequenceNumber(1L).build();
        PersistentEvent event2 = PersistentEventFixture.persistentEvent("test").sequenceNumber(2L).build();
        dao.write(asList(event1, event2));

        Source<List<PersistentEvent>, NotUsed> source = EventsDaoSource.create(new JdbcEventsDao(), "test", 0, FiniteDuration.create(2000, TimeUnit.MILLISECONDS));

        source.map(i -> {
            System.out.println(">>> " + i);
            return i;
        }).runWith(Sink.seq(), actorTestKit.system()).toCompletableFuture().get();
    }
}