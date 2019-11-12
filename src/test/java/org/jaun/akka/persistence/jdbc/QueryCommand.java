package org.jaun.akka.persistence.jdbc;

import akka.actor.typed.ActorRef;

public interface QueryCommand<T> extends Command {
    ActorRef<T> replyTo();
}
