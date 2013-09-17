package dbservice;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * GenericDAO<EntityType>
 * Abstract class that provides a generic implementation for DAO's to extend.
 * Enables most DAO method implementations to be just a single super call.
 *
 * NOTE: Make sure that interfaces are still being used where the DIP is not to be
 * violated. Service classes (or any class, really) should not be dependent on
 * the methods of this class, but rather an interface that defines the CRUD
 * operations of the DAO in DSL terms.
 *
 * @author Joe Rankin
 */
public abstract class GenericDAO<T> {//<T extends IGenericEntity> for future use cases?

    DBAccessor db;

    public GenericDAO() {
    }

    public GenericDAO(final DBAccessor db) {
        this.db = db;
    }

    /**
     * Saves an entity to the specified table based on the given primary key
     * field.
     *
     * @param tableName database table to save to.
     * @param entity entity to save.
     * @param primaryKeyName table's primary key.
     * @throws DbAccessException on error writing to database.
     */
    public final void save(final String tableName, final T entity, final String primaryKeyName) 
            throws DbAccessException, IllegalArgumentException {
        if(tableName == null || tableName.isEmpty() || 
                entity == null || 
                primaryKeyName == null || primaryKeyName.isEmpty()){
            throw new IllegalArgumentException();
        }
        
        Map<String, Object> fields = this.mapFromEntity(entity);
        for (Iterator<String> it = fields.keySet().iterator(); it.hasNext();) {
            String key = it.next();
            if (fields.get(key) == null) {
                it.remove();
            }
        }

        try {
            db.openConnection();
            if (!fields.containsKey(primaryKeyName) || fields.get(primaryKeyName) == null) {
                //new record...
                fields.remove(primaryKeyName);//if it is null... remove it.
                db.insertRecord(tableName, fields, true);

            } else {
                //updated record...
                db.updateRecords(tableName, fields, primaryKeyName, fields.get(primaryKeyName), true);
            }
        } catch (SQLException ex) {
            throw new DbAccessException(ex.getMessage(), ex);
        } catch (Exception ex) {
            throw new DbAccessException(ex.getMessage(), ex);
        }
    }

    /**
     * Delete a row from the database where the fieldName equals the fieldValue
     * in the database.
     *
     * @param tableName name of the database table
     * @param fieldName name of the database column
     * @param fieldValue value of the database column
     * @throws DbAccessException on database error.
     */
    public final void delete(final String tableName, final String fieldName, 
            final Object fieldValue) throws DbAccessException, IllegalArgumentException {
        if(tableName == null || tableName.isEmpty()){
            throw new IllegalArgumentException();
        }
        try {
            db.openConnection();
            db.deleteRecords(tableName, fieldName, fieldValue, true);
        } catch (SQLException ex) {
            throw new DbAccessException(ex.getMessage(), ex);
        } catch (Exception ex) {
            throw new DbAccessException(ex.getMessage(), ex);
        }

    }

    /**
     * Finds a record in the database where fieldName equals fieldValue.
     *
     * @param tableName name of the database table.
     * @param fieldName name of the field in question.
     * @param fieldValue value of the field in question.
     * @return entity where fieldName equals fieldValue.
     * @throws DbAccessException on database error.
     */
    public final T findWhere(final String tableName, final String fieldName, 
            final Object fieldValue) throws DbAccessException, IllegalArgumentException {
        if(tableName == null || tableName.isEmpty() || 
                fieldName == null || fieldName.isEmpty() || 
                fieldValue == null){
            throw new IllegalArgumentException();
        }
        Map<String, Object> record = null;
        try {
            db.openConnection();
            record = db.getRecordWhere(tableName, fieldName, fieldValue, true);
        } catch (SQLException ex) {
            throw new DbAccessException(ex.getMessage(), ex);
        } catch (ClassNotFoundException ex) {
            throw new DbAccessException(ex.getMessage(), ex);
        }
        
        if(record == null){
            return null;
        }
        return this.entityFromMap(record);
    }
    
    public final List<T> findAllWhere(final String tableName, final String fieldName,
            final Object fieldValue) throws DbAccessException, IllegalArgumentException{
        if(tableName == null || tableName.isEmpty() || 
                fieldName == null || fieldName.isEmpty() || 
                fieldValue == null){
            throw new IllegalArgumentException();
        }
        List<Map<String, Object>> records = null;
        try{
            db.openConnection();
            records = db.getRecordsWhere(tableName, fieldName, fieldValue, true);
        } catch(SQLException ex){
            throw new DbAccessException(ex.getMessage(), ex);
        } catch (ClassNotFoundException ex) {
            throw new DbAccessException(ex.getMessage(), ex);
        }
        
        return this.entitiesFromMap(records) ;
    }
    

    /**
     * Executes a SQL select statement that returns records.
     *
     * @param sql the SQL statement to execute
     * @return records returned from the SQL statement executed.
     * @throws DbAccessException on database error.
     */
    public final List<T> getRecordsFromSQL(final String sql) 
            throws DbAccessException, IllegalArgumentException {
        if(sql == null || sql.isEmpty()){
            throw new IllegalArgumentException();
        }
        List<T> records = new ArrayList<T>();
        try {
            db.openConnection();
            List<Map<String, Object>> rawData = new ArrayList<Map<String, Object>>();
            rawData = db.findRecordsFromSQL(sql, true);

            records = entitiesFromMap(rawData);
        } catch (SQLException ex) {
            throw new DbAccessException(ex.getMessage(), ex);
        } catch (ClassNotFoundException ex) {
            throw new DbAccessException(ex.getMessage(), ex);
        } catch (Exception ex) {
            throw new DbAccessException(ex.getMessage(), ex);
        }
        return records;
    }
    
    /**
     * Executes an insert/update/delete statement from raw SQL.
     * 
     * @param sql raw SQL statement to execute
     * @throws DbAccessException on database access error.
     */
    public final void modifyRecordsFromSQL(final String sql) 
            throws DbAccessException, IllegalArgumentException {
        if(sql == null || sql.isEmpty()){
            throw new IllegalArgumentException();
        }
        try {
            db.openConnection();
            
            db.updateRecordsFromSQL(sql, true);
        } catch (SQLException ex) {
            throw new DbAccessException(ex.getMessage(), ex);
        } catch (ClassNotFoundException ex) {
            throw new DbAccessException(ex.getMessage(), ex);
        }
    }

    public DBAccessor getDb() {
        return db;
    }

    public void setDb(final DBAccessor db) throws IllegalArgumentException {
        if(db == null){
            throw new IllegalArgumentException();
        }
        this.db = db;
    }

    /**
     * Convenience method to convert a list of maps to a list of entities.
     *
     * @param dataList list of maps.
     * @return list of entities.
     */
    public final List<T> entitiesFromMap(final List<Map<String, Object>> dataList) {
        List<T> entities = new ArrayList<T>();
        for (Map m : dataList) {
            entities.add(entityFromMap(m));
        }
        return entities;
    }

    /**
     * Convenience method to convert a list of entities to a list of maps.
     *
     * @param entities list of entities.
     * @return list of maps.
     */
    public final List<Map<String, Object>> mapsFromEntities(final List<T> entities) {
        List<Map<String, Object>> maps = new ArrayList<Map<String, Object>>();
        for (T entity : entities) {
            maps.add(mapFromEntity(entity));
        }
        return maps;
    }

    /**
     * Implementations of this method should convert a Map<String,Object>
     * containing database columns and values to an entity.
     * 
     * @param data map of database columns/values.
     * @return an entity.
     */
    public abstract T entityFromMap(final Map<String, Object> data);

    /**
     * Implementations of this method should convert an entity to a 
     * Map<String,Object> containing database columns/values.
     * @param entity an entity
     * @return a map of database columns/values.
     */
    public abstract Map<String, Object> mapFromEntity(final T entity);
}
