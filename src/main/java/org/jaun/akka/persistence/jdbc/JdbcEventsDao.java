package org.jaun.akka.persistence.jdbc;

import com.google.gson.Gson;
import org.apache.commons.dbcp2.BasicDataSource;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class JdbcEventsDao {

    public static final String DB_URL = "jdbc:h2:mem:testdb;LOCK_MODE=1;USER=admin;PASSWORD=admin;DB_CLOSE_DELAY=-1";
    private static final String INSERT_EVENT = "insert into event(stream, seq_number, event_type, tags, metadata, event_data, deleted) values(?, ?, ?, ?, ?, ?, ?)";
    private static final String MAX_SEQ_NR = "select max(seq_number) from event where stream = ?";
    private static final String EVENTS_BY_STREAM = "select stream, seq_number, event_type, tags, metadata, event_data, deleted from event where stream = ? AND seq_number >= ? and seq_number <= ?";

    private BasicDataSource dataSource;
    private Gson gson = new Gson();

    JdbcEventsDao() {
        dataSource = new BasicDataSource();
        dataSource.setUrl(DB_URL);
        dataSource.setUsername("admin");
        dataSource.setPassword("admin");
    }

    public void write(PersistentEvent persistentEvent) {

        String concatenatedTags = persistentEvent.getTags().stream().collect(Collectors.joining(", "));
        String metadata = gson.toJson(persistentEvent.getMetadata());

        try (Connection conn = dataSource.getConnection(); PreparedStatement stmt = conn.prepareStatement(INSERT_EVENT)) {

            stmt.setString(1, persistentEvent.getStream());
            stmt.setLong(2, persistentEvent.getSequenceNumber());
            stmt.setString(3, persistentEvent.getEventType());
            stmt.setString(4, concatenatedTags);
            stmt.setString(5, metadata);
            stmt.setString(6, new String(persistentEvent.getSerializedEvent(), StandardCharsets.UTF_8)); // TODO: just now
            stmt.setBoolean(7, persistentEvent.isDeleted());

            stmt.execute();

            conn.commit();

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    // TODO: handle max (database dependent: https://www.w3schools.com/sql/sql_top.asp)
    public void replay(String stream, long fromSequenceNr, long toSequenceNr, long max, Consumer<PersistentEvent> replayCallback) {

        try (Connection conn = dataSource.getConnection(); PreparedStatement stmt = conn.prepareStatement(EVENTS_BY_STREAM)) {

            stmt.setString(1, stream);
            stmt.setLong(2, fromSequenceNr);
            stmt.setLong(3, toSequenceNr);

            ResultSet resultSet = stmt.executeQuery();

            while (resultSet.next()) {

                String eventData = resultSet.getString("event_data"); // TODO: change to byte
                long seqNumber = resultSet.getLong("seq_number");
                String eventType = resultSet.getString("event_type");
                String commaSeparatedTags = resultSet.getString("tags");
                String metadata = resultSet.getString("metadata");
                boolean deleted = resultSet.getBoolean("deleted");

                Set<String> tags;
                if (commaSeparatedTags != null) {
                    tags = Stream.of(commaSeparatedTags.split(",")).map(s -> s.trim()).collect(Collectors.toSet());
                } else {
                    tags = Collections.emptySet();
                }

                // TODO
                Map<String, String> metadataMap;
                if (metadata != null) {
                    metadataMap = gson.fromJson(metadata, HashMap.class);
                } else {
                    metadataMap = Collections.emptyMap();
                }

                replayCallback.accept(
                        PersistentEvent.builder()
                                .stream(stream)
                                .serializedEvent(eventData.getBytes(StandardCharsets.UTF_8))
                                .sequenceNumber(seqNumber)
                                .eventType(eventType)
                                .tags(tags)
                                .metadata(metadataMap)
                                .deleted(deleted).build());
            }

        } catch (SQLException ex) {
            throw new IllegalStateException(ex);
        }
    }


    public long readHighestSequenceNr(String persistenceId) {

        try (Connection conn = dataSource.getConnection(); PreparedStatement stmt = conn.prepareStatement(MAX_SEQ_NR)) {

            stmt.setString(1, persistenceId);
            ResultSet resultSet = stmt.executeQuery();
            resultSet.next();
            return resultSet.getLong(1);

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
