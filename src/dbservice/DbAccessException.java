package dbservice;

/**
 * An example of a custom exception class that could be used for all
 * database operations. 
 * 
 * @author jlombardo
 */
public class DbAccessException extends Exception {
    public DbAccessException(String msg) {
        super(msg);
    }
    
    public DbAccessException(String msg, Throwable cause) {
        super(msg, cause);
    }
}
