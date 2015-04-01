/**
 * Copyright (c) 2010 Yahoo! Inc. All rights reserved.                                                                                                                             
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you                                                                                                             
 * may not use this file except in compliance with the License. You                                                                                                                
 * may obtain a copy of the License at                                                                                                                                             
 *
 * http://www.apache.org/licenses/LICENSE-2.0                                                                                                                                      
 *
 * Unless required by applicable law or agreed to in writing, software                                                                                                             
 * distributed under the License is distributed on an "AS IS" BASIS,                                                                                                               
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or                                                                                                                 
 * implied. See the License for the specific language governing                                                                                                                    
 * permissions and limitations under the License. See accompanying                                                                                                                 
 * LICENSE file. 
 */

package com.yahoo.ycsb.db;

import com.google.gson.*;
import com.yahoo.ycsb.*;

import java.lang.reflect.Type;
import java.sql.*;
import java.util.*;

/**
 * A class that wraps a JDBC compliant database to allow it to be interfaced with YCSB.
 * This class extends {@link DB} and implements the database interface used by YCSB client.
 * <p/>
 * <br> Each client will have its own instance of this class. This client is
 * not thread safe.
 * <p/>
 * <br> This interface expects a schema <key> <field1> <field2> <field3> ...
 * All attributes are of type VARCHAR. All accesses are through the primary key. Therefore,
 * only one index on the primary key is needed.
 * <p/>
 * <p> The following options must be passed when using this database client.
 * <p/>
 * <ul>
 * <li><b>db.driver</b> The JDBC driver class to use.</li>
 * <li><b>db.url</b> The Database connection URL.</li>
 * <li><b>db.user</b> User name for the connection.</li>
 * <li><b>db.passwd</b> Password for the connection.</li>
 * </ul>
 *
 * @author sudipto
 */
public class JdbcJsonDBClient extends DB implements JdbcDBClientConstants {

	private Connection connection;
	private boolean initialized = false;
	private Integer jdbcFetchSize;
	private static final String DEFAULT_PROP = "";

	private Gson gson;
	private PreparedStatement insertStatement;
	private PreparedStatement readStatement;
	private PreparedStatement deleteStatement;
	private PreparedStatement updateStatement;
	private PreparedStatement scanStatement;

	private class RandomByteIteratorSerializer implements JsonSerializer<RandomByteIterator> {
		@Override
		public JsonElement serialize(RandomByteIterator src, Type typeOfSrc, JsonSerializationContext context)
		{
			return new JsonPrimitive(src.toString());
		}
	}

	/**
	 * Initialize the database connection and set it up for sending requests to the database.
	 * This must be called once per client.
	 */
	@Override
	public void init() throws DBException {
		if( initialized ) {
			System.err.println("Client connection already initialized.");
			return;
		}
		Properties props = getProperties();
		String url = props.getProperty(CONNECTION_URL, DEFAULT_PROP);
		String user = props.getProperty(CONNECTION_USER, DEFAULT_PROP);
		String passwd = props.getProperty(CONNECTION_PASSWD, DEFAULT_PROP);
		String driver = props.getProperty(DRIVER_CLASS);

		String jdbcFetchSizeStr = props.getProperty(JDBC_FETCH_SIZE);
		if( jdbcFetchSizeStr != null ) {
			try {
				this.jdbcFetchSize = Integer.parseInt(jdbcFetchSizeStr);
			} catch( NumberFormatException nfe ) {
				System.err.println("Invalid JDBC fetch size specified: " + jdbcFetchSizeStr);
				throw new DBException(nfe);
			}
		}

		String autoCommitStr = props.getProperty(JDBC_AUTO_COMMIT, Boolean.TRUE.toString());
		Boolean autoCommit = Boolean.parseBoolean(autoCommitStr);

		try {
			if( driver != null ) {
				Class.forName(driver);
			}
			connection = DriverManager.getConnection(url, user, passwd);
			connection.setAutoCommit(autoCommit);
			System.out.println("Connected");
		} catch( ClassNotFoundException e ) {
			System.err.println("Error in initializing the JDBS driver: " + e);
			throw new DBException(e);
		} catch( SQLException e ) {
			System.err.println("Error in database operation: " + e);
			throw new DBException(e);
		} catch( NumberFormatException e ) {
			System.err.println("Invalid value for fieldcount property. " + e);
			throw new DBException(e);
		}
		gson =
			new GsonBuilder()
				.registerTypeAdapter(RandomByteIterator.class, new RandomByteIteratorSerializer())
				.create();
		initialized = true;
	}

	@Override
	public void cleanup() throws DBException {
		try {
			this.connection.close();
		} catch( SQLException e ) {
			System.err.println("Error in closing the connection. " + e);
			throw new DBException(e);
		}
	}

	private PreparedStatement createInsertStatement(String tableName)
		throws SQLException {
		StringBuilder insert = new StringBuilder("INSERT INTO ");
		insert.append(tableName);
		insert.append(" (\"");
		insert.append(PRIMARY_KEY);
		insert.append("\", \"");
		insert.append(COLUMN_NAME);
		insert.append("\")");
		insert.append(" VALUES(?,?::jsonb);");
		return connection.prepareStatement(insert.toString());
	}

	private PreparedStatement createReadStatement(String tableName)
		throws SQLException {
		StringBuilder read = new StringBuilder("SELECT * FROM ");
		read.append(tableName);
		read.append(" WHERE \"");
		read.append(PRIMARY_KEY);
		read.append("\" = ");
		read.append("?;");
		return connection.prepareStatement(read.toString());
	}

	private PreparedStatement createDeleteStatement(String tableName)
		throws SQLException {
		StringBuilder delete = new StringBuilder("DELETE FROM ");
		delete.append(tableName);
		delete.append(" WHERE \"");
		delete.append(PRIMARY_KEY);
		delete.append("\" = ?;");
		return connection.prepareStatement(delete.toString());
	}

	private PreparedStatement createUpdateStatement(String tableName)
		throws SQLException {
		StringBuilder update = new StringBuilder("UPDATE ");
		update.append(tableName);
		update.append(" SET \"");
		update.append(COLUMN_NAME);
		update.append("\"=?::jsonb");
		update.append(" WHERE \"");
		update.append(PRIMARY_KEY);
		update.append("\" = ?;");
		return connection.prepareStatement(update.toString());
	}

	private PreparedStatement createScanStatement(String tableName)
		throws SQLException {
		StringBuilder select = new StringBuilder("SELECT * FROM ");
		select.append(tableName);
		select.append(" WHERE \"");
		select.append(PRIMARY_KEY);
		select.append("\" >= ");
		select.append("? LIMIT ?;");
		PreparedStatement scanStatement = connection.prepareStatement(select.toString());
		if( this.jdbcFetchSize != null ) scanStatement.setFetchSize(this.jdbcFetchSize);
		return scanStatement;
	}

	@Override
	public int read(String tableName, String key, Set<String> fields,
	                HashMap<String, ByteIterator> result) {
		if( tableName == null ) {
			return -1;
		}
		if( key == null ) {
			return -1;
		}
		try {
			if( readStatement == null ) {
				readStatement = createReadStatement(tableName);
			}
			readStatement.setString(1, key);
			ResultSet resultSet = readStatement.executeQuery();
			if( !resultSet.next() ) {
				resultSet.close();
				return 1;
			}

			if( result != null && fields != null ) {
				HashMap<String, String> map = (HashMap<String, String>)gson.fromJson(resultSet.getString(COLUMN_NAME), result.getClass());
				for( String field : fields ) {
					result.put(field, new StringByteIterator(map.get(field)));
				}
			}
			resultSet.close();
			return SUCCESS;
		} catch( SQLException e ) {
			System.err.println("Error in processing read of table " + tableName + ": " + e);
			return -2;
		}
	}

	@Override
	public int scan(String tableName, String startKey, int recordcount,
	                Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
		if( tableName == null ) {
			return -1;
		}
		if( startKey == null ) {
			return -1;
		}
		try {
			if( scanStatement == null ) {
				scanStatement = createScanStatement(tableName);
			}
			scanStatement.setString(1, startKey);
			scanStatement.setInt(2, recordcount);
			ResultSet resultSet = scanStatement.executeQuery();
			for( int i = 0; i < recordcount && resultSet.next(); i++ ) {
				if( result != null && fields != null ) {
					HashMap<String, String> map = (HashMap<String, String>)gson.fromJson(resultSet.getString(COLUMN_NAME), HashMap.class);
					HashMap<String, ByteIterator> values = new HashMap<String, ByteIterator>();
					for( String field : fields ) {
						values.put(field, new StringByteIterator(map.get(field)));
					}
					result.add(values);
				}
			}
			resultSet.close();
			return SUCCESS;
		} catch( SQLException e ) {
			System.err.println("Error in processing scan of table: " + tableName + e);
			return -2;
		}
	}

	@Override
	public int update(String tableName, String key, HashMap<String, ByteIterator> values) {
		if( tableName == null ) {
			return -1;
		}
		if( key == null ) {
			return -1;
		}
		try {
			if( updateStatement == null ) {
				updateStatement = createUpdateStatement(tableName);
			}
			updateStatement.setString(1, gson.toJson(values));
			updateStatement.setString(2, key);
			if( updateStatement.executeUpdate() == 1 ) {
				return SUCCESS;
			} else {
				return 1;
			}
		} catch( SQLException e ) {
			System.err.println("Error in processing update to table: " + tableName + e);
			return -1;
		}
	}

	@Override
	public int insert(String tableName, String key, HashMap<String, ByteIterator> values) {
		if( tableName == null ) {
			return -1;
		}
		if( key == null ) {
			return -1;
		}
		try {
			if( insertStatement == null ) {
				insertStatement = createInsertStatement(tableName);
			}
			insertStatement.setString(1, key);
			insertStatement.setString(2, gson.toJson(values));
//			System.err.println(insertStatement.toString());
			if( insertStatement.executeUpdate() == 1 ) {
				return SUCCESS;
			} else {
				return 1;
			}
		} catch( SQLException e ) {
			System.err.println("Error in processing insert to table: " + tableName + e);
			return -1;
		}
	}

	@Override
	public int delete(String tableName, String key) {
		if( tableName == null ) {
			return -1;
		}
		if( key == null ) {
			return -1;
		}
		try {
			if( deleteStatement == null ) {
				deleteStatement = createDeleteStatement(tableName);
			}
			deleteStatement.setString(1, key);
			if( deleteStatement.executeUpdate() == 1 ) {
				return SUCCESS;
			} else {
				return 1;
			}
		} catch( SQLException e ) {
			System.err.println("Error in processing delete to table: " + tableName + e);
			return -1;
		}
	}
}
