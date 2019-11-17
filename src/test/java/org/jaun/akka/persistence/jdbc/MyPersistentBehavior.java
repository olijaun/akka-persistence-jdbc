package org.jaun.akka.persistence.jdbc;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.persistence.typed.PersistenceId;
import akka.persistence.typed.javadsl.CommandHandler;
import akka.persistence.typed.javadsl.EventHandler;
import akka.persistence.typed.javadsl.EventSourcedBehavior;

import java.util.*;

public class MyPersistentBehavior extends EventSourcedBehavior<Command, Event, MyPersistentBehavior.State> {

    private final List<Event> handledEvents = new ArrayList<>();

    public enum Ack {
        INSTANCE
    }

    public static class TestCommand implements Command {
        private final String value;
        private final ActorRef<Ack> replyTo;

        public TestCommand(ActorRef<Ack> replyTo, String value) {
            this.value = value;
            this.replyTo = replyTo;
        }

        public String getValue() {
            return value;
        }

        private ActorRef<Ack> replyTo() {
            return replyTo;
        }

        @Override
        public String toString() {
            return "TestCommand{" +
                    "value='" + value + '\'' +
                    '}';
        }
    }

    public static class QueryHandledEvents implements QueryCommand<List<Event>> {

        private final ActorRef<List<Event>> replyTo;

        public QueryHandledEvents(ActorRef<List<Event>> replyTo) {
            this.replyTo = replyTo;
        }

        public ActorRef<List<Event>> replyTo() {
            return replyTo;
        }

    }

    public static class TestEvent implements Event {
        private final String value;

        public TestEvent(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }

        @Override
        public String toString() {
            return "TestEvent{" +
                    "value='" + value + '\'' +
                    '}';
        }
    }

    public static class State {
    }

    public static Behavior<Command> create(UUID id) {
        return new MyPersistentBehavior(PersistenceId.ofUniqueId(id.toString()));
    }

    private MyPersistentBehavior(PersistenceId persistenceId) {
        super(persistenceId);
    }

    @Override
    public State emptyState() {
        return new State();
    }

    @Override
    public CommandHandler<Command, Event, State> commandHandler() {

        return newCommandHandlerBuilder()
                .forAnyState()
                .onCommand(TestCommand.class, command -> {
                    return Effect().persist(new TestEvent(command.getValue())) //
                            .thenReply(command.replyTo(), state -> Ack.INSTANCE);
                })
                .onCommand(QueryHandledEvents.class, command -> {
                    return Effect().none() //
                            .thenReply(command.replyTo(), state -> handledEvents);
                })
                .build();
    }

    @Override
    public EventHandler<State, Event> eventHandler() {
        return (state, event) -> {
            handledEvents.add(event);
            return null;
        };
    }

    @Override
    public Set<String> tagsFor(Event event) {
        HashSet<String> tags = new HashSet<>();
        tags.add("tag1");
        tags.add("tag2");

        return tags;
    }
}
