package org.jaun.akka.persistence.jdbc;

import akka.dispatch.Futures;
import akka.persistence.AtomicWrite;
import akka.persistence.PersistentImpl;
import akka.persistence.PersistentRepr;
import akka.persistence.journal.Tagged;
import akka.persistence.journal.japi.AsyncWriteJournal;
import akka.serialization.Serialization;
import akka.serialization.SerializationExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConversions;
import scala.collection.immutable.Seq;
import scala.concurrent.Future;
import scala.util.Try;

import java.util.*;
import java.util.function.Consumer;

// https://doc.akka.io/docs/akka/current/persistence-journals.html
// https://github.com/kpavlov/akka-custom-persistence/blob/21c98661a8ddbd0c0041dde9a02d588d6a4eb597/src/main/java/example/persistence/WorkerJournal.java
public class JdbcAsyncWriteJournal extends AsyncWriteJournal {

    private static final String AKKA_WRITER_UUID = "akka.persistentRepr.writerUuid";
    private static final String AKKA_SENDER = "akka.persistentRepr.sender";

    private Serialization serialization;
    private final Logger logger = LoggerFactory.getLogger(JdbcAsyncWriteJournal.class);
    private final JdbcEventsDao jdbcEventsDao;

    JdbcAsyncWriteJournal() {
        serialization = SerializationExtension.get(context().system());
        jdbcEventsDao = new JdbcEventsDao();
    }

    public Future<Void> doAsyncReplayMessages(String persistenceId, long fromSequenceNr, long toSequenceNr,
                                              long max, Consumer<PersistentRepr> replayCallback) {

        Consumer<PersistedEvent> consumer = persistedEvent -> {
            replayCallback.accept(toPersistentRepr(persistedEvent));
        };

        jdbcEventsDao.replay(persistenceId, fromSequenceNr, toSequenceNr, max, consumer);

        return Future.successful(null);
    }

    private PersistentRepr toPersistentRepr(PersistedEvent event) {

        String writerUuid = event.getMetadata().get(AKKA_WRITER_UUID);
        String sender = event.getMetadata().get(AKKA_SENDER); // TODO: how to convert this back to an actor ref?

        Object deserializedEvent;
        try {
            Class<?> aClass = Thread.currentThread().getContextClassLoader().loadClass(event.getEventType());

            Try<?> deserialize = serialization.deserialize(event.getSerializedEvent(), aClass);

            if (deserialize.isFailure()) {
                throw new IllegalStateException(deserialize.failed().get());
            }

            deserializedEvent = deserialize.get();
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException(e);
        }

        return new PersistentImpl(deserializedEvent, event.getSequenceNumber(), event.getStream(), event.getEventType(), event.isDeleted(), null, writerUuid);
    }


    public Future<Long> doAsyncReadHighestSequenceNr(String persistenceId, long l) {

        long highestSequenceNr = jdbcEventsDao.readHighestSequenceNr(persistenceId);

        return Futures.successful(highestSequenceNr);
    }

    public Future<Iterable<Optional<Exception>>> doAsyncWriteMessages(Iterable<AtomicWrite> messages) {

        ArrayList<Optional<Exception>> result = new ArrayList<Optional<Exception>>();

        logger.info("Writing: {}", messages);
        for (AtomicWrite message : messages) {

            try {
                final Seq<PersistentRepr> persistentReprSeq = message.payload();
                persistentReprSeq.foreach(persistentRepr -> {

                    Object payload;
                    Set<String> tags;
                    if (persistentRepr.payload() instanceof Tagged) {
                        Tagged tagged = (Tagged) persistentRepr.payload();
                        payload = tagged.payload();
                        tags = JavaConversions.setAsJavaSet(tagged.tags());
                    } else {
                        payload = persistentRepr.payload();
                        tags = Collections.emptySet();
                    }

                    Try<byte[]> serializedPayload = serialization.serialize(payload);

                    if (serializedPayload.isFailure()) {
                        throw new IllegalArgumentException("serialization failed: " + serializedPayload.failed().get());
                    }

                    String sender = null;
                    if (persistentRepr.sender() != null) {
                        sender = persistentRepr.sender().path().toSerializationFormat();
                    }

                    String manifest;
                    if (persistentRepr.manifest().equals("")) {
                        manifest = payload.getClass().getName();
                    } else {
                        manifest = persistentRepr.manifest();
                    }

                    HashMap<String, String> metadataMap = new HashMap<>();
                    metadataMap.put(AKKA_WRITER_UUID, persistentRepr.writerUuid());
                    metadataMap.put(AKKA_SENDER, sender);

                    PersistedEvent persistedEvent = PersistedEvent.builder()
                            .stream(persistentRepr.persistenceId())
                            .serializedEvent(serializedPayload.get())
                            .sequenceNumber(persistentRepr.sequenceNr())
                            .eventType(manifest)
                            .tags(tags)
                            .metadata(metadataMap)
                            .deleted(persistentRepr.deleted()).build();

                    jdbcEventsDao.write(persistedEvent);

                    return null;
                });

                result.add(Optional.empty());

            } catch (Exception e) {
                result.add(Optional.of(e));
            }
        }
        return Futures.successful(result);
    }

    public Future<Void> doAsyncDeleteMessagesTo(String s, long l) {
        throw new UnsupportedOperationException("Method is not implemented: doAsyncDeleteMessagesTo");

    }
}
