package org.jaun.akka.persistence.jdbc;

import akka.actor.testkit.typed.javadsl.ActorTestKit;
import akka.actor.testkit.typed.javadsl.TestProbe;
import akka.actor.typed.ActorRef;
import com.typesafe.config.ConfigFactory;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

class JdbcAsyncWriteJournalIT {


    @BeforeAll
    private static void beforeAll() throws Exception {

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

    @AfterAll
    private static void afterAll() throws Exception {
        try ( //
              Connection conn = DriverManager.getConnection(JdbcEventsDao.DB_URL, "admin", "admin"); //
              Statement stmt = conn.createStatement()) {

            ResultSet resultSet = stmt.executeQuery("select * from event;");

            int columnCount = resultSet.getMetaData().getColumnCount();

            for (int i = 1; i <= columnCount; i++) {
                System.out.print(resultSet.getMetaData().getColumnLabel(i) + "\t\t\t | ");
            }
            System.out.println();

            while (resultSet.next()) {

                for (int i = 1; i <= columnCount; i++) {
                    System.out.print(resultSet.getString(i) + "\t\t\t | ");
                }
                System.out.println();
            }
            System.out.println();
        }
    }

    /**
     * just write
     */
    @Test
    void doAsyncWriteMessages() throws InterruptedException {

        ActorTestKit actorTestKit = ActorTestKit.create("test", ConfigFactory.defaultApplication());

        UUID persistenceId = UUID.randomUUID();

        ActorRef<Command> myPersistentActorWrite = actorTestKit.spawn(MyPersistentBehavior.create(persistenceId));

        TestProbe<MyPersistentBehavior.Ack> testProbe = actorTestKit.createTestProbe("ack");

        myPersistentActorWrite.tell(new MyPersistentBehavior.TestCommand(testProbe.getRef(), "do write test"));

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

        ActorTestKit actorTestKit = ActorTestKit.create("test", ConfigFactory.defaultApplication());
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