package org.jaun.akka.persistence.jdbc;

import akka.dispatch.Futures;
import akka.persistence.AtomicWrite;
import akka.persistence.PersistentRepr;
import akka.persistence.journal.japi.AsyncWriteJournal;
import akka.serialization.Serialization;
import akka.serialization.SerializationExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConversions;
import scala.collection.immutable.Seq;
import scala.concurrent.Future;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.Collectors;

// https://doc.akka.io/docs/akka/current/persistence-journals.html
// https://github.com/kpavlov/akka-custom-persistence/blob/21c98661a8ddbd0c0041dde9a02d588d6a4eb597/src/main/java/example/persistence/WorkerJournal.java
public class JdbcAsyncWriteJournal extends AsyncWriteJournal {


    private Serialization serialization;
    private final Logger logger = LoggerFactory.getLogger(JdbcAsyncWriteJournal.class);
    private final JdbcEventsDao jdbcEventsDao;

    JdbcAsyncWriteJournal() {
        serialization = SerializationExtension.get(context().system());
        jdbcEventsDao = new JdbcEventsDao();
    }

    public Future<Void> doAsyncReplayMessages(String persistenceId, long fromSequenceNr, long toSequenceNr,
                                              long max, Consumer<PersistentRepr> replayCallback) {

        Iterable<PersistentEventWithOffset> events = jdbcEventsDao.readByStream(persistenceId, fromSequenceNr, toSequenceNr, max);
        events.forEach(event -> replayCallback.accept(Converter.toPersistentRepr(serialization, event.getEvent())));

        return Future.successful(null);
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

                List<PersistentEvent> list = JavaConversions.seqAsJavaList(persistentReprSeq) //
                        .stream() //
                        .map(e -> Converter.toPersistentEvent(serialization, e)) //
                        .collect(Collectors.toList());

                jdbcEventsDao.write(list);

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
