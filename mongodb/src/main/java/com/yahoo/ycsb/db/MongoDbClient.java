/**
 * MongoDB client binding for YCSB.
 *
 * Submitted by Yen Pai on 5/11/2010.
 *
 * https://gist.github.com/000a66b8db2caf42467b#file_mongo_db.java
 *
 */

package com.yahoo.ycsb.db;

import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.concurrent.atomic.AtomicInteger;

import com.mongodb.*;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import com.yahoo.ycsb.ByteArrayByteIterator;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import org.bson.Document;

/**
 * MongoDB client for YCSB framework.
 * 
 * Properties to set:
 * 
 * mongodb.url=mongodb://localhost:27017 mongodb.database=ycsb
 * mongodb.writeConcern=normal
 * 
 * @author ypai
 */
public class MongoDbClient extends DB {

    /** Used to include a field in a response. */
    protected static final Integer INCLUDE = 1;

    /** A singleton Mongo instance. */
    private static MongoClient mongo;

    /** The default write concern for the test. */
    private static WriteConcern writeConcern;

    /** The database to access. */
    private static String database;

    /** Count the number of times initialized to teardown on the last {@link #cleanup()}. */
    private static final AtomicInteger initCount = new AtomicInteger(0);

    /**
     * Initialize any state for this DB.
     * Called once per DB instance; there is one DB instance per client thread.
     */
    @Override
    public void init() throws DBException {
	    Logger mongoLogger = Logger.getLogger( "com.mongodb" );
	    mongoLogger.setLevel(Level.WARNING);

        initCount.incrementAndGet();
        synchronized (INCLUDE) {
            if (mongo != null) {
                return;
            }

            // initialize MongoDb driver
            Properties props = getProperties();
            String url = props.getProperty("mongodb.url",
                    "mongodb://localhost:27017");
            database = props.getProperty("mongodb.database", "ycsb");
            String writeConcernType = props.getProperty("mongodb.writeConcern",
                    "safe").toLowerCase();
            final String maxConnections = props.getProperty(
                    "mongodb.maxconnections", "10");

            if ("safe".equals(writeConcernType)) {
                writeConcern = WriteConcern.SAFE;
            }
            else if ("normal".equals(writeConcernType)) {
                writeConcern = WriteConcern.NORMAL;
            }
            else if ("fsync_safe".equals(writeConcernType)) {
                writeConcern = WriteConcern.FSYNC_SAFE;
            }
            else if ("replicas_safe".equals(writeConcernType)) {
                writeConcern = WriteConcern.REPLICAS_SAFE;
            }
            else {
                System.err
                        .println("ERROR: Invalid writeConcern: '"
                                + writeConcernType
                                + "'. "
                                + "Must be [ none | safe | normal | fsync_safe | replicas_safe ]");
                System.exit(1);
            }

            try {
	            // strip out prefix since Java driver doesn't currently support
	            // standard connection format URL yet
	            // http://www.mongodb.org/display/DOCS/Connections
	            if (url.startsWith("mongodb://")) {
		            url = url.substring(10);
	            }

                // need to append db to url.
//                url += "/" + database;
                System.out.println("new database url = " + url);
	            MongoClientOptions options = MongoClientOptions.builder().connectionsPerHost(Integer.parseInt(maxConnections)).build();
	            mongo = new MongoClient(url, options);

                System.out.println("mongo connection created with " + url);
            }
            catch (Exception e1) {
                System.err
                        .println("Could not initialize MongoDB connection pool for Loader: "
                                + e1.toString());
                e1.printStackTrace();
            }
        }
    }

    /**
     * Cleanup any state for this DB.
     * Called once per DB instance; there is one DB instance per client thread.
     */
    @Override
    public void cleanup() throws DBException {
        if (initCount.decrementAndGet() <= 0) {
            try {
                mongo.close();
            }
            catch (Exception e1) {
                System.err.println("Could not close MongoDB connection pool: "
                        + e1.toString());
                e1.printStackTrace();
            }
        }
    }

    /**
     * Delete a record from the database.
     *
     * @param table The name of the table
     * @param key The record key of the record to delete.
     * @return Zero on success, a non-zero error code on error. See this class's description for a discussion of error codes.
     */
    @Override
    public int delete(String table, String key) {
        MongoDatabase db = null;
        try {
            db = mongo.getDatabase(database);
	        MongoCollection<Document> collection = db.getCollection(table);
	        DeleteResult res = collection.deleteOne(new Document("_id", key));
            return res.wasAcknowledged() ? 0: 1;
        }
        catch (Exception e) {
            System.err.println(e.toString());
            return 1;
        }
    }

    /**
     * Insert a record in the database. Any field/value pairs in the specified values HashMap will be written into the record with the specified
     * record key.
     *
     * @param table The name of the table
     * @param key The record key of the record to insert.
     * @param values A HashMap of field/value pairs to insert in the record
     * @return Zero on success, a non-zero error code on error. See this class's description for a discussion of error codes.
     */
    @Override
    public int insert(String table, String key,
            HashMap<String, ByteIterator> values) {
	    MongoDatabase db = null;
        try {
	        db = mongo.getDatabase(database);
	        MongoCollection<Document> collection = db.getCollection(table);
	        Document doc = new Document("_id", key);
	        for (String k : values.keySet()) {
		        doc.put(k, values.get(k).toArray());
	        }
	        collection.insertOne(doc);
	        return 0;
        }
        catch (Exception e) {
            e.printStackTrace();
            return 1;
        }
    }

    /**
     * Read a record from the database. Each field/value pair from the result will be stored in a HashMap.
     *
     * @param table The name of the table
     * @param key The record key of the record to read.
     * @param fields The list of fields to read, or null for all of them
     * @param result A HashMap of field/value pairs for the result
     * @return Zero on success, a non-zero error code on error or "not found".
     */
    @Override
    @SuppressWarnings("unchecked")
    public int read(String table, String key, Set<String> fields,
            HashMap<String, ByteIterator> result) {
	    MongoDatabase db = null;
        try {
	        db = mongo.getDatabase(database);
	        MongoCollection<Document> collection = db.getCollection(table);

	        Document queryResult = null;
	        try {
		        queryResult = collection.find(new Document("_id", key)).first();
	        } catch( Exception e ) {
		        // skip
	        }
            if (queryResult != null) {
                result.putAll(new LinkedHashMap(queryResult));
            }
            return queryResult != null ? 0 : 1;
        }
        catch (Exception e) {
            System.err.println(e.toString());
            return 1;
        }
    }

    /**
     * Update a record in the database. Any field/value pairs in the specified values HashMap will be written into the record with the specified
     * record key, overwriting any existing values with the same field name.
     *
     * @param table The name of the table
     * @param key The record key of the record to write.
     * @param values A HashMap of field/value pairs to update in the record
     * @return Zero on success, a non-zero error code on error. See this class's description for a discussion of error codes.
     */
    @Override
    public int update(String table, String key,
            HashMap<String, ByteIterator> values) {
	    MongoDatabase db = null;
        try {
	        db = mongo.getDatabase(database);
	        MongoCollection<Document> collection = db.getCollection(table);
	        Document doc = new Document();
	        for( String tmpKey : values.keySet() ) {
		        doc.put(tmpKey, values.get(tmpKey).toArray());
	        }
	        UpdateResult res = collection.updateOne(new Document("_id", key), new Document("$set", doc));
            return res.wasAcknowledged() ? 0 : 1;
        }
        catch (Exception e) {
            System.err.println(e.toString());
            return 1;
        }
    }

    /**
     * Perform a range scan for a set of records in the database. Each field/value pair from the result will be stored in a HashMap.
     *
     * @param table The name of the table
     * @param startkey The record key of the first record to read.
     * @param recordcount The number of records to read
     * @param fields The list of fields to read, or null for all of them
     * @param result A Vector of HashMaps, where each HashMap is a set field/value pairs for one record
     * @return Zero on success, a non-zero error code on error. See this class's description for a discussion of error codes.
     */
    @Override
    public int scan(String table, String startkey, int recordcount,
            Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
	    MongoDatabase db = null;
        try {
	        db = mongo.getDatabase(database);
	        MongoCollection<Document> collection = db.getCollection(table);
	        for( Document document : collection.find(new Document("_id", new Document("$gte", startkey)))
		        .limit(recordcount) ) {
		        HashMap<String, ByteIterator> resultMap = new HashMap<String, ByteIterator>();
		        fillHashMap(resultMap, document);
		        result.add(resultMap);
	        }

            return 0;
        }
        catch (Exception e) {
            System.err.println(e.toString());
            return 1;
        }

    }

    /**
     * TODO - Finish
     * 
     * @param resultMap
     * @param obj
     */
    @SuppressWarnings("unchecked")
    protected void fillMap(HashMap<String, ByteIterator> resultMap, DBObject obj) {
        Map<String, Object> objMap = obj.toMap();
        for (Map.Entry<String, Object> entry : objMap.entrySet()) {
            if (entry.getValue() instanceof byte[]) {
                resultMap.put(entry.getKey(), new ByteArrayByteIterator(
                        (byte[]) entry.getValue()));
            }
        }
    }

    /**
     * TODO - Finish
     *
     * @param resultMap
     * @param obj
     */
    @SuppressWarnings("unchecked")
    protected void fillHashMap(HashMap<String, ByteIterator> resultMap, Document obj) {
        for (Map.Entry<String, Object> entry : obj.entrySet()) {
            if (entry.getValue() instanceof byte[]) {
                resultMap.put(entry.getKey(), new ByteArrayByteIterator(
                        (byte[]) entry.getValue()));
            }
        }
    }
}
