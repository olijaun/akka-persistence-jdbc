package org.jaun.akka.persistence.jdbc;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

public interface StatementExecutor {
    ResultSet run(Connection connection) throws SQLException;
}
