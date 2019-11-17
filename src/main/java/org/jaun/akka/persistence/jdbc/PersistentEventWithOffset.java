package org.jaun.akka.persistence.jdbc;

import java.util.Objects;

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

    @Override
    public String toString() {
        return "PersistentEventWithOffset{" +
                "event=" + event +
                ", offset=" + offset +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PersistentEventWithOffset that = (PersistentEventWithOffset) o;
        return offset == that.offset &&
                event.equals(that.event);
    }

    @Override
    public int hashCode() {
        return Objects.hash(event, offset);
    }
}
