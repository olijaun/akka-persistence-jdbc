package org.jaun.akka.persistence.jdbc;

import akka.NotUsed;
import akka.actor.ExtendedActorSystem;
import akka.persistence.query.EventEnvelope;
import akka.persistence.query.Offset;
import akka.persistence.query.javadsl.*;
import akka.serialization.Serialization;
import akka.serialization.SerializationExtension;
import akka.stream.javadsl.Source;
import com.typesafe.config.Config;
import scala.concurrent.duration.FiniteDuration;

import java.util.List;
import java.util.Set;
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

        Source<List<PersistentEventWithOffset>, NotUsed> source = PersistentEventSource.create(new JdbcEventsDao(), "test", 0, FiniteDuration.create(2000, TimeUnit.MILLISECONDS));
        return source.flatMapConcat(persistentEventList -> Source.from(persistentEventList)).map(this::toEventEnvelope);
    }

    @Override
    public Source<EventEnvelope, NotUsed> currentEventsByPersistenceId(String persistenceId, long fromSequenceNr, long toSequenceNr) {
        Iterable<PersistentEventWithOffset> iterable = eventsDao.readByStream(persistenceId, fromSequenceNr, toSequenceNr, Long.MAX_VALUE);
        return Source.from(iterable).map(this::toEventEnvelope);
    }

    private EventEnvelope toEventEnvelope(PersistentEventWithOffset persistentEventWithOffset) {

        Object payloadEvent = Converter.toPayload(serialization, persistentEventWithOffset.getEvent());

        return new EventEnvelope(Offset.sequence(persistentEventWithOffset.getOffset()), persistentEventWithOffset.getEvent().getStream(), persistentEventWithOffset.getEvent().getSequenceNumber(), payloadEvent);
    }

    @Override
    public Source<EventEnvelope, NotUsed> currentEventsByTag(String tag, Offset offset) {
        return null;
    }

    @Override
    public Source<String, NotUsed> currentPersistenceIds() {
        return Source.from(eventsDao.persistenceIds());
    }

    @Override
    public Source<EventEnvelope, NotUsed> eventsByTag(String tag, Offset offset) {
        return null;
    }

    @Override
    public Source<String, NotUsed> persistenceIds() {
        Source<Set<String>, NotUsed> source = PersistenceIdSource.create(new JdbcEventsDao(),
                FiniteDuration.create(2000, TimeUnit.MILLISECONDS));

        return source.flatMapConcat(persistenceSet -> Source.from(persistenceSet));
    }

    public long refreshInterval() {
        return refreshInterval.length();
    }
}
