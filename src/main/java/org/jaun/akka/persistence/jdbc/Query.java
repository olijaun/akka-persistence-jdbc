package org.jaun.akka.persistence.jdbc;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;

public class Query {

    private final DataSource dataSource;

    Query(DataSource dataSource) {
        this.dataSource = Objects.requireNonNull(dataSource);
    }

    public <T> Iterable<T> run(StatementExecutor statementExecutor, RowMapper<T> mapper) {
        return () -> new ResultSetIterator<>(statementExecutor, mapper);
    }

    public class ResultSetIterator<T> implements Iterator<T> {

        private StatementExecutor statementExecutor;
        private ResultSet resultSet;
        private Connection connection;
        private RowMapper<T> rowMapper;
        private int rowNum;
        private Boolean hasNextWasSuccessfull;

        public ResultSetIterator(StatementExecutor statementExecutor, RowMapper<T> rowMapper) {
            this.statementExecutor = statementExecutor;
            this.rowMapper = rowMapper;
            this.rowNum = 0;
            this.hasNextWasSuccessfull = false;
        }

        private void runStatement() {
            try {
                if (resultSet == null) {
                    connection = dataSource.getConnection();
                    resultSet = statementExecutor.run(connection);
                    resultSet.beforeFirst();
                }
            } catch (SQLException e) {
                throw new IllegalStateException(e);
            }
        }

        @Override
        public boolean hasNext() {

            runStatement();

            try {

                if (!connection.isClosed() && (hasNextWasSuccessfull || resultSet.next())) {
                    hasNextWasSuccessfull = true;
                    return true;
                } else {
                    hasNextWasSuccessfull = false;
                    System.out.println("close()");
                    connection.close();
                    return false;
                }

            } catch (SQLException e) {
                throw new IllegalStateException(e);
            }
        }

        @Override
        public T next() {

            runStatement();

            try {

                if (hasNext()) {
                    rowNum++;
                    hasNextWasSuccessfull = false;
                    return rowMapper.mapRow(resultSet, rowNum);

                } else {
                    if (!connection.isClosed()) {
                        connection.close();
                    }
                    throw new NoSuchElementException();
                }
            } catch (SQLException e) {
                throw new IllegalStateException(e);
            }
        }
    }
}
