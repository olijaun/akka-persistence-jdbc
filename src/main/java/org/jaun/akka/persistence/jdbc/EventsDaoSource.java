package org.jaun.akka.persistence.jdbc;

import akka.NotUsed;
import akka.stream.Attributes;
import akka.stream.Outlet;
import akka.stream.SourceShape;
import akka.stream.javadsl.Source;
import akka.stream.stage.*;
import scala.concurrent.duration.FiniteDuration;
import scala.util.Try;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * https://stackoverflow.com/questions/49708397/create-source-from-a-polling-method-in-akka
 * https://github.com/akka/alpakka/blob/c1315a8d1979b4399b62db726c159d64149501f7/file/src/main/java/akka/stream/alpakka/file/javadsl/DirectoryChangesSource.java#L33-L170
 * https://akka.io/blog/2016/08/29/connecting-existing-apis
 * https://gist.github.com/johanandren/41b096c9ee647863c6c04959be548b25
 *
 */
public final class EventsDaoSource extends GraphStage<SourceShape<List<PersistentEvent>>> {

    private final JdbcEventsDao eventsDao;
    private final long fromSequenceNumber;
    private final Outlet<List<PersistentEvent>> out = Outlet.create("EventsDaoSource.out");
    private final SourceShape<List<PersistentEvent>> shape = SourceShape.of(out);
    private final FiniteDuration pollingInterval;
    private final String persistenceId;

    public EventsDaoSource(JdbcEventsDao eventsDao, String persistenceId, long fromSequenceNumber, FiniteDuration pollingInterval) {
        this.eventsDao = eventsDao;
        this.fromSequenceNumber = fromSequenceNumber <= 0 ? 1 : fromSequenceNumber;
        this.pollingInterval = pollingInterval;
        this.persistenceId = persistenceId;
    }

    @Override
    public SourceShape<List<PersistentEvent>> shape() {
        return shape;
    }

    @Override
    public GraphStageLogic createLogic(Attributes inheritedAttributes) throws IOException {

        return new TimerGraphStageLogic(shape) {

            private long position = fromSequenceNumber;
            private AsyncCallback<Try<Integer>> chunkCallback;

            {
                setHandler(out, new AbstractOutHandler() {
                    @Override
                    public void onPull() throws Exception {
                        doPull();
                    }
                });
            }

            @Override
            public void preStart() {
            }

            @Override
            public void onTimer(Object timerKey) {
                doPull();
            }

            private void doPull() {

                try {
                    Iterable<PersistentEvent> persistentEvents = eventsDao.read(persistenceId, position, Long.MAX_VALUE, Long.MAX_VALUE);
                    List<PersistentEvent> events = StreamSupport.stream(persistentEvents.spliterator(), false).collect(Collectors.toList());

                    if(events.size() > 0) {
                        position += events.size();
                        push(out, events);

                    } else {
                        scheduleOnce("poll", pollingInterval);
                    }

                } catch (Exception e) {
                    failStage(e);
                }
            }

            @Override
            public void postStop() {
                // close stuff
            }
        };
    }

    // factory methods
    public static Source<List<PersistentEvent>, NotUsed> create(JdbcEventsDao eventsDao, String persistenceId, long fromSequenceNumber, FiniteDuration pollingInterval) {
        return Source.fromGraph(new EventsDaoSource(eventsDao, persistenceId, fromSequenceNumber, pollingInterval));
    }
}