package org.jaun.akka.persistence.jdbc;

import org.apache.commons.dbcp2.BasicDataSource;

import javax.sql.DataSource;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.*;
import java.util.stream.Collectors;

public class TestDb {

    private static final String DB_URL = JdbcEventsDao.DB_URL;

    private static boolean setup = false;
    private static BasicDataSource dataSource;

    public static synchronized void setup() {

        try ( //
              Connection conn = DriverManager.getConnection(DB_URL, null, null); //
              Statement stmt = conn.createStatement()) {

            stmt.executeUpdate("DROP ALL OBJECTS");

            // create tables
            InputStream inputStream = JdbcAsyncWriteJournal.class.getResourceAsStream("/eventstore.ddl");
            String ddl = new BufferedReader(new InputStreamReader(inputStream))
                    .lines().collect(Collectors.joining("\n"));
            stmt.executeUpdate(ddl);
            setup = true;

            conn.commit();

        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }

    public static void printTables() {
        try ( //
              Connection conn = getDataSource().getConnection(); //
              Statement stmt = conn.createStatement()) {

            ResultSet resultSet = stmt.executeQuery("select * from event;");

            int columnCount = resultSet.getMetaData().getColumnCount();

            for (int i = 1; i <= columnCount; i++) {
                System.out.print(resultSet.getMetaData().getColumnLabel(i) + "\t\t\t | ");
            }
            System.out.println();

            while (resultSet.next()) {

                for (int i = 1; i <= columnCount; i++) {
                    System.out.print(resultSet.getString(i) + "\t\t\t | ");
                }
                System.out.println();
            }
            System.out.println();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static DataSource getDataSource() {

        if (dataSource == null) {
            dataSource = new BasicDataSource();
            dataSource.setUrl(JdbcEventsDao.DB_URL);
            dataSource.setUsername("admin");
            dataSource.setPassword("admin");
        }
        return dataSource;
    }
}
