package dbservice;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

/**
 * Implementations provide a way to perform CRUD operations using a database.
 * @author Joe Rankin
 */
public interface DBAccessor {
    /**
     * Opens a connection to the database.
     * @return the opened connection.
     * @throws SQLException on database error.
     * @throws ClassNotFoundException if driver class not found.
     */
    public abstract Connection openConnection() throws SQLException, ClassNotFoundException;
    
    /**
     * Closes the connection to the database.
     * @throws SQLException on database error.
     */
    public abstract void closeConnection() throws SQLException;
    
    /**
     * Inserts a record into the database from a Map<String,Object>.
     * @param tableName name of the database table.
     * @param fields map of column names to their values.
     * @param closeConnection whether or not to close the connection.
     * @return success boolean
     * @throws SQLException on database error
     * @throws Exception on error
     */
    public abstract boolean insertRecord(String tableName, Map<String, Object> fields, 
            boolean closeConnection) throws SQLException, Exception;
    
    /**
     * Returns a List of records as Map<String,Object>s.
     * @param sql SQL query to execute.
     * @param closeConection whether or not to close the connection.
     * @return List of records as Map<String,Object>s.
     * @throws SQLException on database error.
     * @throws Exception on error.
     */
    public abstract List<Map<String,Object>> findRecordsFromSQL(String sql, boolean closeConection) 
            throws SQLException, Exception;
    
    /**
     * Updates records in the database from raw SQL.
     * @param sql SQL statement to update record(s).
     * @param closeConnection whether or not to close the connection.
     * @throws SQLException on database error.
     */
    public abstract void updateRecordsFromSQL(String sql, boolean closeConnection)
            throws SQLException;
    
    /**
     * Gets a record based on a where field from the specified table.
     * @param table database table
     * @param fieldName where field name
     * @param fieldValue where field value
     * @param closeConnection whether or not to close the connection
     * @return a record as a Map<String,Object>
     * @throws SQLException on database error
     */
    public abstract Map<String, Object> getRecordWhere(String table, String fieldName, 
            Object fieldValue, boolean closeConnection) throws SQLException;
    
    /**
     * Gets a list of records where fieldName equals fieldValue.
     * @param table database table
     * @param fieldName where field name
     * @param fieldValue where field value
     * @param closeConnection whether or not to close the connection
     * @return records as a List<Map<String, Object>>
     * @throws SQLException on database error
     */
    public abstract List<Map<String,Object>> getRecordsWhere(String table, String fieldName,
            Object fieldValue, boolean closeConnection) throws SQLException;
    
    /**
     * Update a record to match fields where whereField = whereValue.
     * @param tableName database table name
     * @param fields fields to update.
     * @param whereField name of the where column
     * @param whereValue value of the where column
     * @param closeConnection whether or not to close the connection
     * @return number of records updated.
     * @throws SQLException on database error
     * @throws Exception on error
     */
    public abstract int updateRecords(String tableName, Map<String, Object> fields, 
            String whereField, Object whereValue, boolean closeConnection) throws SQLException, Exception;
    
    /**
     * Delete records based on a where field from a table.
     * @param tableName name of database table
     * @param whereField where field name
     * @param whereValue where field value
     * @param closeConnection whether or not to close the connection
     * @return number of records updated.
     * @throws SQLException on database error
     * @throws Exception on error
     */
    public abstract int deleteRecords(String tableName, String whereField, Object whereValue,
            boolean closeConnection) throws SQLException, Exception;
    
}
