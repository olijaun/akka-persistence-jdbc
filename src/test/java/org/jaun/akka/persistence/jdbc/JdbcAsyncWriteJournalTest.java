package org.jaun.akka.persistence.jdbc;

import akka.actor.testkit.typed.javadsl.ActorTestKit;
import akka.actor.testkit.typed.javadsl.TestProbe;
import akka.actor.typed.ActorRef;
import akka.actor.typed.ActorSystem;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.Behaviors;
import com.typesafe.config.ConfigFactory;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.util.stream.Collectors;

class JdbcAsyncWriteJournalTest {


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
        }
    }

    @Test
    void empty() throws Exception {
    }

    @Test
    void doAsyncWriteMessages() throws InterruptedException {

        ActorTestKit actorTestKit = ActorTestKit.create("test", ConfigFactory.defaultApplication());

        Behavior<MyPersistentBehavior.Command> myPersistentBehavior = MyPersistentBehavior.create("123");

        ActorRef<MyPersistentBehavior.Command> myPersistentActorWrite = actorTestKit.spawn(myPersistentBehavior);

        TestProbe<MyPersistentBehavior.Ack> testProbe = actorTestKit.createTestProbe("ack");

        myPersistentActorWrite.tell(new MyPersistentBehavior.TestCommand(testProbe.getRef(), "do something"));

        testProbe.expectMessage(Duration.ofSeconds(5), MyPersistentBehavior.Ack.INSTANCE);

        ActorRef<MyPersistentBehavior.Command> myPersistentActorRead = actorTestKit.spawn(myPersistentBehavior);

    }

    @Test
    void doAsyncReplayMessages() throws InterruptedException {

        // prepare
        ActorTestKit actorTestKit = ActorTestKit.create("test", ConfigFactory.defaultApplication());
        Behavior<MyPersistentBehavior.Command> myPersistentBehavior = MyPersistentBehavior.create("123");
        TestProbe<MyPersistentBehavior.Ack> testProbe = actorTestKit.createTestProbe("ack");
        ActorRef<MyPersistentBehavior.Command> myPersistentActorWrite = actorTestKit.spawn(myPersistentBehavior);

        // run
        myPersistentActorWrite.tell(new MyPersistentBehavior.TestCommand(testProbe.getRef(), "do something"));

        // verify
        testProbe.expectMessage(Duration.ofSeconds(5), MyPersistentBehavior.Ack.INSTANCE);

        TestProbe<Event> testProbeEvent = actorTestKit.createTestProbe("ack");

//        ActorRef<Event> myPersistentActorRead = actorTestKit.spawn
//                (Behaviors.monitor(Event.class, testProbeEvent.getRef(), myPersistentBehavior));

        testProbe.expectMessage(Duration.ofSeconds(5), MyPersistentBehavior.Ack.INSTANCE);
    }
}