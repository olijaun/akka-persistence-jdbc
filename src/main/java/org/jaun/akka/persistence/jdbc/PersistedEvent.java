package org.jaun.akka.persistence.jdbc;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class PersistedEvent {

    private final String stream;
    private final long sequenceNumber;
    private final String eventType;
    private final Set<String> tags;
    private final Map<String, String> metadata;
    private final byte[] serializedEvent;
    private final boolean deleted;

    private PersistedEvent(Builder builder) {
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

        public PersistedEvent build() {
            return new PersistedEvent(this);
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
            this.tags = tags;
            return this;
        }

        public Builder metadata(Map<String, String> metadata) {
            this.metadata = metadata;
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
