package org.jaun.akka.persistence.jdbc;

import java.util.*;

import static java.util.Objects.requireNonNull;

public class PersistentEvent {

    private final String stream;
    private final long sequenceNumber;
    private final String eventType;
    private final Set<String> tags;
    private final Map<String, String> metadata;
    private final byte[] serializedEvent;
    private final boolean deleted;

    private PersistentEvent(Builder builder) {
        this.stream = requireNonNull(builder.stream);
        this.sequenceNumber = requireNonNull(builder.sequenceNumber);
        this.eventType = requireNonNull(builder.eventType);
        this.tags = requireNonNull(builder.tags);
        this.metadata = requireNonNull(builder.metadata);
        this.serializedEvent = requireNonNull(builder.serializedEvent);
        this.deleted = requireNonNull(builder.deleted);
    }

    public String getStream() {
        return stream;
    }

    public long getSequenceNumber() {
        return sequenceNumber;
    }

    public String getEventType() {
        return eventType;
    }

    public Set<String> getTags() {
        return tags;
    }

    public Map<String, String> getMetadata() {
        return metadata;
    }

    public byte[] getSerializedEvent() {
        return serializedEvent;
    }

    public boolean isDeleted() {
        return deleted;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PersistentEvent event = (PersistentEvent) o;
        return sequenceNumber == event.sequenceNumber &&
                deleted == event.deleted &&
                stream.equals(event.stream) &&
                eventType.equals(event.eventType) &&
                tags.equals(event.tags) &&
                metadata.equals(event.metadata) &&
                Arrays.equals(serializedEvent, event.serializedEvent);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(stream, sequenceNumber, eventType, tags, metadata, deleted);
        result = 31 * result + Arrays.hashCode(serializedEvent);
        return result;
    }

    @Override
    public String toString() {
        return "PersistentEvent{" +
                "stream='" + stream + '\'' +
                ", sequenceNumber=" + sequenceNumber +
                ", eventType='" + eventType + '\'' +
                ", tags=" + tags +
                ", metadata=" + metadata +
                ", serializedEvent=<not shown>" +
                ", deleted=" + deleted +
                '}';
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String stream;
        private Long sequenceNumber;
        private String eventType;
        private Set<String> tags = new HashSet<>();
        private Map<String, String> metadata = new HashMap<>();
        private byte[] serializedEvent;
        private Boolean deleted;

        public PersistentEvent build() {
            return new PersistentEvent(this);
        }

        public Builder stream(String strema) {
            this.stream = strema;
            return this;
        }

        public Builder sequenceNumber(Long sequenceNumber) {
            this.sequenceNumber = sequenceNumber;
            return this;
        }

        public Builder eventType(String eventType) {
            this.eventType = eventType;
            return this;
        }

        public Builder tags(Set<String> tags) {
            if(tags == null) {
                this.tags = new HashSet<>();
            } else {
                this.tags = tags;
            }
            return this;
        }

        public Builder metadata(Map<String, String> metadata) {
            if(metadata == null) {
                this.metadata = new HashMap<>();
            } else {
                this.metadata = metadata;
            }
            return this;
        }

        public Builder serializedEvent(byte[] serializedEvent) {
            this.serializedEvent = serializedEvent;
            return this;
        }

        public Builder deleted(Boolean deleted) {
            this.deleted = deleted;
            return this;
        }
    }
}
