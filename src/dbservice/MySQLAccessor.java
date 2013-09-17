package dbservice;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Provides a way to perform CRUD operations on a MySQL database.
 * @author Joe Rankin
 */
public class MySQLAccessor implements DBAccessor {

    private String driverClassName;
    private String url;
    private String username;
    private String password;
    private Connection conn;

    public MySQLAccessor(String driverClassName, String url, String username, String password) {
        String msg = "Error: url is null or zero length!";
        if (url == null || url.length() == 0) {
            throw new IllegalArgumentException(msg);
        }
        this.driverClassName = driverClassName;
        this.url = url;
        this.username = (username == null) ? "" : username;
        this.password = (password == null) ? "" : password;
    }

    @Override
    public Connection openConnection() throws ClassNotFoundException, SQLException {
        //Class.forName(driverClassName);
        Class.forName(driverClassName, true, this.getClass().getClassLoader());
        conn = DriverManager.getConnection(url, username, password);
        return conn;
    }

    @Override
    public void closeConnection() throws SQLException {
        conn.close();
    }
    
    public void closeConnection(boolean closeConnection) throws SQLException{
        if(closeConnection){
            closeConnection();
        }
    }

    @Override
    public List<Map<String, Object>> findRecordsFromSQL(String sql, boolean closeConnection) throws SQLException, Exception {
        Statement stmt = null;
        ResultSet rs;
        ResultSetMetaData metaData;
        final List<Map<String, Object>> records = new ArrayList();
        Map<String, Object> record;

        try {
            stmt = conn.createStatement();
            rs = stmt.executeQuery(sql);
            metaData = rs.getMetaData();
            final int fields = metaData.getColumnCount();

            while (rs.next()) {
                record = new HashMap<String, Object>();
                for (int i = 1; i <= fields; i++) {
                    try {
                        record.put(metaData.getColumnName(i), rs.getObject(i));
                    } catch (NullPointerException npe) {
                        // no need to do anything... if it fails, just ignore it and continue
                    }
                }
                records.add(record);
            }
        } finally {
            try {
                if (stmt != null) {
                    stmt.close();
                }
                closeConnection(closeConnection);
            } finally {
            }
        }
        return records;
    }
    
    @Override
    public void updateRecordsFromSQL(String sql, boolean closeConnection) throws SQLException {
        Statement stmt = null;
        int recsUpdated = 0;
        try{
            stmt = conn.createStatement();
            stmt.executeUpdate(sql);
        } finally{
            try {
                if (stmt != null) {
                    stmt.close();
                }
                if (closeConnection && conn != null) {
                    conn.close();
                }
            } finally { }
        }
    }

    @Override
    public boolean insertRecord(String tableName, Map<String, Object> fields, boolean closeConnection) throws SQLException, Exception {

        PreparedStatement pstmt = null;
        int recsUpdated = 0;

        try {
            pstmt = buildInsertStatement(conn, tableName, new ArrayList(fields.keySet()));

            final Iterator i = fields.values().iterator();
            int index = 1;
            while (i.hasNext()) {
                final Object obj = i.next();
                if (obj instanceof String) {
                    pstmt.setString(index++, (String) obj);
                } else if (obj instanceof Integer) {
                    pstmt.setInt(index++, ((Integer) obj).intValue());
                } else if (obj instanceof Long) {
                    pstmt.setLong(index++, ((Long) obj).longValue());
                } else if (obj instanceof Double) {
                    pstmt.setDouble(index++, ((Double) obj).doubleValue());
                } else if (obj instanceof java.sql.Date) {
                    pstmt.setDate(index++, (java.sql.Date) obj);
                } else if (obj instanceof Boolean) {
                    pstmt.setBoolean(index++, ((Boolean) obj).booleanValue());
                } else {
                    if (obj != null) {
                        pstmt.setObject(index++, obj);
                    }
                }
            }
            recsUpdated = pstmt.executeUpdate();
        } finally {
            try {
                if (pstmt != null) {
                    pstmt.close();
                }
                closeConnection(closeConnection);
            } finally {
            }
        }

        return recsUpdated == 1;
    }

    @Override
    public int updateRecords(String tableName, Map<String, Object> fields,
            String whereField, Object whereValue, boolean closeConnection) throws SQLException, Exception {

        PreparedStatement pstmt = null;
        int recsUpdated = 0;
        
        try {
            pstmt = buildUpdateStatement(conn, tableName,
                    new ArrayList(fields.keySet()), whereField);
            final Iterator i = fields.values().iterator();
            int index = 1;
            boolean doWhereValueFlag = false;
            Object obj = null;

            while (i.hasNext() || doWhereValueFlag) {
                if (!doWhereValueFlag) {
                    obj = i.next();
                }

                if (obj instanceof String) {
                    pstmt.setString(index++, (String) obj);
                } else if (obj instanceof Integer) {
                    pstmt.setInt(index++, ((Integer) obj).intValue());
                } else if (obj instanceof Long) {
                    pstmt.setLong(index++, ((Long) obj).longValue());
                } else if (obj instanceof Double) {
                    pstmt.setDouble(index++, ((Double) obj).doubleValue());
                } else if (obj instanceof java.sql.Timestamp) {
                    pstmt.setTimestamp(index++, (java.sql.Timestamp) obj);
                } else if (obj instanceof java.sql.Date) {
                    pstmt.setDate(index++, (java.sql.Date) obj);
                } else if (obj instanceof Boolean) {
                    pstmt.setBoolean(index++, ((Boolean) obj).booleanValue());
                } else {
                    if (obj != null) {
                        pstmt.setObject(index++, obj);
                    }
                }

                if (doWhereValueFlag) {
                    break;
                }
                if (!i.hasNext()) {
                    doWhereValueFlag = true;
                    obj = whereValue;
                }
            }
            
            //System.out.println(pstmt.toString());//debug
            recsUpdated = pstmt.executeUpdate();

        } finally {
            try {
                if(pstmt != null){
                    pstmt.close();
                }
                closeConnection(closeConnection);
            } catch (SQLException ex) {
                throw ex;
            }
        }

        return recsUpdated;
    }

    @Override
    public int deleteRecords(String tableName, String whereField, Object whereValue,
            boolean closeConnection) throws SQLException, Exception {
        PreparedStatement pstmt = null;
        int recsDeleted = 0;

        try {
            pstmt = buildDeleteStatement(conn, tableName, whereField);
            // delete all records if whereField is null
            if (whereField != null) {
                if (whereValue instanceof String) {
                    pstmt.setString(1, (String) whereValue);
                } else if (whereValue instanceof Integer) {
                    pstmt.setInt(1, ((Integer) whereValue).intValue());
                } else if (whereValue instanceof Long) {
                    pstmt.setLong(1, ((Long) whereValue).longValue());
                } else if (whereValue instanceof Double) {
                    pstmt.setDouble(1, ((Double) whereValue).doubleValue());
                } else if (whereValue instanceof java.sql.Date) {
                    pstmt.setDate(1, (java.sql.Date) whereValue);
                } else if (whereValue instanceof Boolean) {
                    pstmt.setBoolean(1, ((Boolean) whereValue).booleanValue());
                } else {
                    if (whereValue != null) {
                        pstmt.setObject(1, whereValue);
                    }
                }
            }

            recsDeleted = pstmt.executeUpdate();

        } finally {
            try {
                if(pstmt != null){
                    pstmt.close();
                }
                closeConnection(closeConnection);
            } catch (SQLException e) {
                throw e;
            }
        }

        return recsDeleted;
    }
    
    /**
     * Gets a record from the database where fieldName = fieldValue.
     * @param table database table to access.
     * @param fieldName name of the where field.
     * @param fieldValue value of the where field.
     * @param closeConnection whether or not to close the connection.
     * @return Map<String, Object> containing a record.
     * @throws SQLException on database error.
     */
    @Override
    public Map<String, Object> getRecordWhere(String table, String fieldName, Object fieldValue, boolean closeConnection) throws SQLException {
        Statement stmt = null;
        ResultSet rs = null;
        ResultSetMetaData metaData = null;
        final Map record = new HashMap();

        try {
            stmt = conn.createStatement();
            String sql2;

            if (fieldValue instanceof String) {
                sql2 = "= '" + fieldValue + "'";
            } else {
                sql2 = "=" + fieldValue;
            }

            final String sql = "SELECT * FROM " + table + " WHERE " + fieldName + sql2;
            rs = stmt.executeQuery(sql);
            metaData = rs.getMetaData();
            metaData.getColumnCount();
            final int fields = metaData.getColumnCount();
            
            if (rs.next()) {
                for (int i = 1; i <= fields; i++) {
                    record.put(metaData.getColumnName(i), rs.getObject(i));
                }
            } else{
                return null;
            }
        } finally {
            try {
                if(stmt != null){
                    stmt.close();
                }
                closeConnection(closeConnection);
            } catch (SQLException ex) {
                throw ex;
            }
        }
        
        return record;
    }
    
    
    
        /**
     * Gets a record from the database where fieldName = fieldValue.
     * @param table database table to access.
     * @param fieldName name of the where field.
     * @param fieldValue value of the where field.
     * @param closeConnection whether or not to close the connection.
     * @return Map<String, Object> containing a record.
     * @throws SQLException on database error.
     */
    @Override
    public List<Map<String, Object>> getRecordsWhere(String table, String fieldName, Object fieldValue, boolean closeConnection) throws SQLException {
        Statement stmt = null;
        ResultSet rs = null;
        ResultSetMetaData metaData = null;
        final List<Map<String, Object>> records = new ArrayList<Map<String, Object>>();

        try {
            stmt = conn.createStatement();
            String sql2;

            if (fieldValue instanceof String) {
                sql2 = "= '" + fieldValue + "'";
            } else {
                sql2 = "=" + fieldValue;
            }

            final String sql = "SELECT * FROM " + table + " WHERE " + fieldName + sql2;
            rs = stmt.executeQuery(sql);
            
            metaData = rs.getMetaData();
            metaData.getColumnCount();
            final int fields = metaData.getColumnCount();
            
            while(rs.next()){
                
                Map<String, Object> record = new HashMap<String, Object>();
                for (int i = 1; i <= fields; i++) {
                    record.put(metaData.getColumnName(i), rs.getObject(i));
                }
                records.add(record);
            }
            
        } finally {
            try {
                if(stmt != null){
                    stmt.close();
                }
                closeConnection(closeConnection);
            } catch (SQLException ex) {
                throw ex;
            }
        }
        
        return records;
    }
    
    

    /**
     * Builds a java.sql.PreparedStatement for an sql insert
     *
     * @author Jim Lombardo
     * @param conn - a valid connection
     * @param tableName - a <code>String</code> representing the table name
     * @param colDescriptors - a <code>List</code> containing the column
     * descriptors for the fields that can be inserted.
     * @return java.sql.PreparedStatement
     * @throws SQLException
     */
    private PreparedStatement buildInsertStatement(Connection conn_loc, String tableName, List colDescriptors)
            throws SQLException {
        StringBuffer sql = new StringBuffer("INSERT INTO ");
        (sql.append(tableName)).append(" (");
        final Iterator i = colDescriptors.iterator();
        while (i.hasNext()) {
            (sql.append((String) i.next())).append(", ");
        }
        sql = new StringBuffer((sql.toString()).substring(0, (sql.toString()).lastIndexOf(", ")) + ") VALUES (");
        for (int j = 0; j < colDescriptors.size(); j++) {
            sql.append("?, ");
        }
        final String finalSQL = (sql.toString()).substring(0, (sql.toString()).lastIndexOf(", ")) + ")";
        return conn_loc.prepareStatement(finalSQL);
    }

    /**
     * Builds a java.sql.PreparedStatement for an sql update using only one
     * where clause test
     *
     * @author Jim Lombardo
     * @param conn - a JDBC <code>Connection</code> object
     * @param tableName - a <code>String</code> representing the table name
     * @param colDescriptors - a <code>List</code> containing the column
     * descriptors for the fields that can be updated.
     * @param whereField - a <code>String</code> representing the field name for
     * the search criteria.
     * @return java.sql.PreparedStatement
     * @throws SQLException
     */
    private PreparedStatement buildUpdateStatement(Connection conn_loc, String tableName,
            List colDescriptors, String whereField)
            throws SQLException {
        StringBuffer sql = new StringBuffer("UPDATE ");
        (sql.append(tableName)).append(" SET ");
        final Iterator i = colDescriptors.iterator();
        while (i.hasNext()) {
            (sql.append((String) i.next())).append(" = ?, ");
        }
        sql = new StringBuffer((sql.toString()).substring(0, (sql.toString()).lastIndexOf(", ")));
        ((sql.append(" WHERE ")).append(whereField)).append(" = ?");
        final String finalSQL = sql.toString();
        return conn_loc.prepareStatement(finalSQL);
    }

    /**
     * Builds a java.sql.PreparedStatement for an sql delete using only one
     * where clause test
     *
     * @author Jim Lombardo
     * @param conn - a JDBC <code>Connection</code> object
     * @param tableName - a <code>String</code> representing the table name
     * @param whereField - a <code>String</code> representing the field name for
     * the search criteria.
     * @return java.sql.PreparedStatement
     * @throws SQLException
     */
    private PreparedStatement buildDeleteStatement(Connection conn_loc, String tableName, String whereField)
            throws SQLException {
        final StringBuffer sql = new StringBuffer("DELETE FROM ");
        sql.append(tableName);

        if (whereField != null) {
            sql.append(" WHERE ");
            (sql.append(whereField)).append(" = ?");
        }

        final String finalSQL = sql.toString();
        return conn_loc.prepareStatement(finalSQL);
    }
}
