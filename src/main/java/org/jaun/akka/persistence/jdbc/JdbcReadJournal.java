package org.jaun.akka.persistence.jdbc;

import akka.NotUsed;
import akka.actor.ExtendedActorSystem;
import akka.persistence.query.EventEnvelope;
import akka.persistence.query.Offset;
import akka.persistence.query.javadsl.*;
import akka.serialization.Serialization;
import akka.serialization.SerializationExtension;
import akka.serialization.Serializers;
import akka.stream.javadsl.Source;
import com.typesafe.config.Config;
import scala.concurrent.duration.FiniteDuration;
import scala.util.Try;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

// https://blog.jooq.org/tag/slick/
// https://stackoverflow.com/questions/44999614/stream-records-from-database-using-akka-stream
public class JdbcReadJournal implements ReadJournal, PersistenceIdsQuery, CurrentPersistenceIdsQuery, EventsByPersistenceIdQuery, CurrentEventsByPersistenceIdQuery, EventsByTagQuery, CurrentEventsByTagQuery {

    private final FiniteDuration refreshInterval;
    private JdbcEventsDao eventsDao;
    private ExtendedActorSystem system;
    private Serialization serialization;

    public JdbcReadJournal(ExtendedActorSystem system, Config config) {
        this.system = system;
        refreshInterval = FiniteDuration.create(config.getDuration("refresh-interval",
                TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS);
        serialization = SerializationExtension.get(system);
        eventsDao = new JdbcEventsDao();
    }

    public static final String Identifier() {
        return "jdbc-read-journal";
    }

    @Override
    public Source<EventEnvelope, NotUsed> eventsByPersistenceId(String persistenceId, long fromSequenceNr, long toSequenceNr) {
        return null;
    }

    @Override
    public Source<EventEnvelope, NotUsed> currentEventsByPersistenceId(String persistenceId, long fromSequenceNr, long toSequenceNr) {

        ArrayList<EventEnvelope> persistentEvents = new ArrayList<>();

        eventsDao.replay(persistenceId, fromSequenceNr, toSequenceNr, Long.MAX_VALUE, e -> persistentEvents.add(toEventEnvelope(e)));

        return Source.from(persistentEvents);
    }

    private EventEnvelope toEventEnvelope(PersistentEvent persistentEvent) {

        Object deserializedEvent;
        try {
            Class<?> aClass = Class.forName(persistentEvent.getEventType());

            Try<?> deserialize = serialization.deserialize(persistentEvent.getSerializedEvent(), aClass);

            if (deserialize.isFailure()) {
                throw new IllegalStateException(deserialize.failed().get());
            }

            deserializedEvent = deserialize.get();
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException(e);
        }

        return new EventEnvelope(Offset.sequence(persistentEvent.getSequenceNumber()), persistentEvent.getStream(), persistentEvent.getSequenceNumber(), deserializedEvent);
    }

    @Override
    public Source<EventEnvelope, NotUsed> currentEventsByTag(String tag, Offset offset) {
        return null;
    }

    @Override
    public Source<String, NotUsed> currentPersistenceIds() {
        return null;
    }

    @Override
    public Source<EventEnvelope, NotUsed> eventsByTag(String tag, Offset offset) {
        return null;
    }

    @Override
    public Source<String, NotUsed> persistenceIds() {
        return null;
    }

    public long refreshInterval() {
        return refreshInterval.length();
    }
}
