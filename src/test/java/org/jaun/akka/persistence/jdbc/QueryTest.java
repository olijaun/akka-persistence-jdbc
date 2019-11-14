package org.jaun.akka.persistence.jdbc;

import com.google.common.truth.Truth;
import javafx.util.Pair;
import org.apache.commons.dbcp2.BasicDataSource;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.*;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.jupiter.api.Assertions.*;

class QueryTest {

    private static JdbcEventsDao dao;
    private static BasicDataSource dataSource;

    @BeforeAll
    private static void beforeAll() throws Exception {

        dataSource = new BasicDataSource();
        dataSource.setUrl(JdbcEventsDao.DB_URL);
        dataSource.setUsername("admin");
        dataSource.setPassword("admin");

        try ( //
              Connection conn = DriverManager.getConnection(JdbcEventsDao.DB_URL, null, null); //
              Statement stmt = conn.createStatement()) {

            conn.setAutoCommit(true);
            stmt.executeUpdate("create table test_table(col1 varchar, col2 int);");

            stmt.executeUpdate("insert into test_table(col1, col2) values('abc', 123)");
            stmt.executeUpdate("insert into test_table(col1, col2) values('def', 456)");
            stmt.executeUpdate("insert into test_table(col1, col2) values('ghi', 789)");

        }
    }

    @Test
    void run() {

        Query query = new Query(dataSource);

        RowMapper<Pair<String, Integer>> rowMapper = (rs, rowNum) -> {
            String col1 = rowNum + ":" + rs.getString("col1");
            Integer col2 = rs.getInt("col2");
            return new Pair<>(col1, col2);
        };

        StatementExecutor executor = conn -> {
            PreparedStatement stmt = conn.prepareStatement("select col1, col2 from test_table where col1=? or col2=? order by col1");
            stmt.setString(1, "abc");
            stmt.setInt(2, 789);
            return stmt.executeQuery();
        };

        Iterator<Pair<String, Integer>> iterable = query.run(executor, rowMapper);

        Pair<String, Integer> pair1 = iterable.next();

        assertThat(pair1.getKey()).isEqualTo("1:abc");
        assertThat(pair1.getValue()).isEqualTo(123);

        Pair<String, Integer> pair2 = iterable.next();

        assertThat(pair2.getKey()).isEqualTo("2:ghi");
        assertThat(pair2.getValue()).isEqualTo(789);

        Executable e = () -> iterable.next();

        assertThrows(NoSuchElementException.class, e);
    }
}