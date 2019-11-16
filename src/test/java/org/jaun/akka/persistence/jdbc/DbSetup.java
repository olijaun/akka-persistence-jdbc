package org.jaun.akka.persistence.jdbc;

import org.apache.commons.dbcp2.BasicDataSource;

import javax.sql.DataSource;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.stream.Collectors;

public class DbSetup {

    private static final String DB_URL = JdbcEventsDao.DB_URL;

    private static boolean setup = false;
    private static BasicDataSource dataSource;

    public static synchronized void setup() {

        if (setup) {
            return;
        }

        InputStream inputStream = JdbcAsyncWriteJournal.class.getResourceAsStream("/eventstore.ddl");

        String ddl = new BufferedReader(new InputStreamReader(inputStream))
                .lines().collect(Collectors.joining("\n"));

        try ( //
              Connection conn = DriverManager.getConnection(DB_URL, null, null); //
              Statement stmt = conn.createStatement()) {

            conn.setAutoCommit(true);

            stmt.executeUpdate(ddl);

            setup = true;

        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }

    public static DataSource getDataSource() {

        setup();

        if(dataSource == null) {
            dataSource = new BasicDataSource();
            dataSource.setUrl(JdbcEventsDao.DB_URL);
            dataSource.setUsername("admin");
            dataSource.setPassword("admin");
        }
        return dataSource;
    }
}
