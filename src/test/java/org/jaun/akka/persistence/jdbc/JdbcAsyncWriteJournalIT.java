package org.jaun.akka.persistence.jdbc;

import akka.actor.testkit.typed.javadsl.ActorTestKit;
import akka.actor.testkit.typed.javadsl.TestProbe;
import akka.actor.typed.ActorRef;
import com.typesafe.config.ConfigFactory;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

class JdbcAsyncWriteJournalIT {

    private static ActorTestKit actorTestKit = ActorTestKit.create("test", ConfigFactory.defaultApplication());

    @BeforeAll
    private static void beforeAll() throws Exception {
        TestDb.setup();
    }

    @AfterAll
    private static void afterAll() throws Exception {

        actorTestKit.shutdownTestKit();

        TestDb.printTables();
    }

    /**
     * just write
     */
    @Test
    void doAsyncWriteMessages() throws InterruptedException {

        // prepare
        UUID persistenceId = UUID.randomUUID();
        ActorRef<Command> myPersistentActorWrite = actorTestKit.spawn(MyPersistentBehavior.create(persistenceId));
        TestProbe<MyPersistentBehavior.Ack> testProbe = actorTestKit.createTestProbe("ack");

        // run
        myPersistentActorWrite.tell(new MyPersistentBehavior.TestCommand(testProbe.getRef(), "do write test"));

        // verify
        testProbe.expectMessage(MyPersistentBehavior.Ack.INSTANCE);
    }

    /**
     * write and then read. we stop the persistent actor after sending the command.
     * then we re-spawn the behavior and check whether the event is replayed by querying the actor.
     */
    @Test
    void doAsyncReplayMessages() throws InterruptedException {

        // prepare
        UUID persistenceId = UUID.randomUUID();

        TestProbe<MyPersistentBehavior.Ack> testProbe = actorTestKit.createTestProbe("ack");
        ActorRef<Command> myPersistentActorWrite = actorTestKit.spawn(MyPersistentBehavior.create(persistenceId));

        MyPersistentBehavior.TestCommand testCommand = new MyPersistentBehavior.TestCommand(testProbe.getRef(), "do read test");
        myPersistentActorWrite.tell(testCommand);
        testProbe.expectMessage(MyPersistentBehavior.Ack.INSTANCE);

        actorTestKit.stop(myPersistentActorWrite);

        // run: spawn triggers a replay of events
        ActorRef<Command> myPersistentActorRead = actorTestKit.spawn(MyPersistentBehavior.create(persistenceId));

        // verify
        TestProbe<List<Event>> queryProbe = actorTestKit.createTestProbe("query");
        myPersistentActorRead.tell(new MyPersistentBehavior.QueryHandledEvents(queryProbe.getRef()));
        List<Event> events = queryProbe.receiveMessage();

        assertEquals(events.size(), 1);
        MyPersistentBehavior.TestEvent testEvent = (MyPersistentBehavior.TestEvent) events.get(0);
        assertEquals(testEvent.getValue(), testCommand.getValue());
    }
}