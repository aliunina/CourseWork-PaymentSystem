package com.webapp.persistence.dao.impl;

import java.sql.*;

import com.mysql.cj.jdbc.Driver;
import com.mysql.cj.jdbc.StatementImpl;

import java.util.ResourceBundle;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.webapp.config.ConnectionPool;

public class QueryExecutor {

	private static final Logger LOGGER = LogManager.getLogger(QueryExecutor.class);

	/**
	 * Connection instance
	 */
	private Connection connection = getConnection();

	/**
	 * PreparedStatement instance
	 */
	private PreparedStatement preparedStatement;

	/**
	 * Singleton instance
	 */
	private static QueryExecutor instance = null;

	/**
	 * Getting connection from connection pool.
	 *
	 * @see ConnectionPool
	 * @throws SQLException
	 */
	private Connection getConnection() {
		ResourceBundle props = ResourceBundle.getBundle("configuration");
		String url = props.getString("URL");
		String username = props.getString("USER");
		String password = props.getString("PASSWORD");

		try {
			Class.forName("com.mysql.jdbc.Driver");
			Driver driver = new com.mysql.cj.jdbc.Driver();
			DriverManager.registerDriver(driver);
			return DriverManager.getConnection(url, username, password);
		} catch (ClassNotFoundException | SQLException e) {
			e.printStackTrace();
		}

        return connection;
	}

	private QueryExecutor() {
	}

	public static QueryExecutor getInstance() {
		if (instance == null)
			instance = new QueryExecutor();
		return instance;
	}

	/**
	 * Inserts an array of objects into prepared statement.
	 *
	 * @param values
	 *            array of objects to be inserted
	 * @throws SQLException
	 */
	private void setValues(Object... values) throws SQLException {
		for (int i = 0; i < values.length; i++) {
			preparedStatement.setObject(i + 1, values[i]);
		}
	}

	/**
	 * Executes insert(returns id), update and delete queries.
	 *
	 * @param query
	 * @param args
	 * @return if request is insert
	 */
	public int executeStatement(String query, Object... args) {
		try {
			preparedStatement = connection.prepareStatement(query, StatementImpl.RETURN_GENERATED_KEYS);
			setValues(args);
			int res = preparedStatement.executeUpdate();
			ResultSet resultSet = preparedStatement.getGeneratedKeys();
			if (resultSet.next()) {
				return resultSet.getInt(1);
			} else {
				return res;
			}
		} catch (SQLException e) {
			LOGGER.error("Execute statement error " + e.getMessage());
		}
		return 0;
	}

	/**
	 * Executes select query and returns resultset.
	 *
	 * @param query
	 *            to be executed
	 * @param args
	 * @return result of select queries
	 * @throws SQLException
	 */
	public ResultSet getResultSet(String query, Object... args) throws SQLException {
		preparedStatement = connection.prepareStatement(query);
		setValues(args);
		return preparedStatement.executeQuery();
	}

	/**
	 * Returns connection to pool.
	 */
	public void closeConnection() {
		try {
			connection.close();
		} catch (SQLException e) {
			LOGGER.error("Error while closing connection");
		}
	}
}
