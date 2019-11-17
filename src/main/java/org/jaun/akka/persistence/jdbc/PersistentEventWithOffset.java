package org.jaun.akka.persistence.jdbc;

import static java.util.Objects.requireNonNull;

public class PersistentEventWithOffset {

    private final PersistentEvent event;
    private final long offset;

    public PersistentEventWithOffset(PersistentEvent event, long offset) {
        this.event = requireNonNull(event);
        this.offset = requireNonNull(offset);
    }

    public PersistentEvent getEvent() {
        return event;
    }

    public long getOffset() {
        return offset;
    }
}
