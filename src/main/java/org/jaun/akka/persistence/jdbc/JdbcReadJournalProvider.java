package org.jaun.akka.persistence.jdbc;

import akka.actor.ExtendedActorSystem;
import akka.persistence.query.ReadJournalProvider;
import akka.persistence.query.javadsl.ReadJournal;
import com.typesafe.config.Config;

public class JdbcReadJournalProvider implements ReadJournalProvider {

    private final JdbcReadJournal jdbcReadJournal;

    public JdbcReadJournalProvider(ExtendedActorSystem system, Config config, String configPath) {
        jdbcReadJournal = new JdbcReadJournal(system, config);
    }

    @Override
    public ReadJournal javadslReadJournal() {
        return jdbcReadJournal;
    }

    @Override
    public akka.persistence.query.scaladsl.ReadJournal scaladslReadJournal() {
        // TODO Auto-generated method stub
        return null;
    }
}
