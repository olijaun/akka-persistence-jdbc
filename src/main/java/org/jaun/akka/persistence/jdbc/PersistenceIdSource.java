package org.jaun.akka.persistence.jdbc;

import akka.NotUsed;
import akka.stream.Attributes;
import akka.stream.Outlet;
import akka.stream.SourceShape;
import akka.stream.javadsl.Source;
import akka.stream.stage.AbstractOutHandler;
import akka.stream.stage.GraphStage;
import akka.stream.stage.GraphStageLogic;
import akka.stream.stage.TimerGraphStageLogic;
import scala.concurrent.duration.FiniteDuration;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * https://stackoverflow.com/questions/49708397/create-source-from-a-polling-method-in-akka
 * https://github.com/akka/alpakka/blob/c1315a8d1979b4399b62db726c159d64149501f7/file/src/main/java/akka/stream/alpakka/file/javadsl/DirectoryChangesSource.java#L33-L170
 * https://akka.io/blog/2016/08/29/connecting-existing-apis
 * https://gist.github.com/johanandren/41b096c9ee647863c6c04959be548b25
 */
public final class PersistenceIdSource extends GraphStage<SourceShape<Set<String>>> {

    private final JdbcEventsDao eventsDao;
    private final Outlet<Set<String>> out = Outlet.create("PersistenceIdSource.out");
    private final SourceShape<Set<String>> shape = SourceShape.of(out);
    private final FiniteDuration pollingInterval;

    public PersistenceIdSource(JdbcEventsDao eventsDao, FiniteDuration pollingInterval) {
        this.eventsDao = eventsDao;
        this.pollingInterval = pollingInterval;
    }

    @Override
    public SourceShape<Set<String>> shape() {
        return shape;
    }

    @Override
    public GraphStageLogic createLogic(Attributes inheritedAttributes) throws IOException {

        return new TimerGraphStageLogic(shape) {

            Set<String> allreadyReadPersistenceIds = new HashSet<>();

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

                    Set<String> newlyReadIds = StreamSupport.stream(eventsDao.persistenceIds().spliterator(), false).collect(Collectors.toSet());

                    newlyReadIds.removeAll(allreadyReadPersistenceIds);

                    if (newlyReadIds.size() > 0) {
                        allreadyReadPersistenceIds.addAll(newlyReadIds);
                        push(out, newlyReadIds);
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
    public static Source<Set<String>, NotUsed> create(JdbcEventsDao eventsDao, FiniteDuration pollingInterval) {
        return Source.fromGraph(new PersistenceIdSource(eventsDao, pollingInterval));
    }
}