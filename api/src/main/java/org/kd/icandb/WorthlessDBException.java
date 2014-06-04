package org.kd.icandb;

/**
 * @author kirk
 */
public class WorthlessDBException extends Exception {

    public WorthlessDBException() {
    }

    public WorthlessDBException(String message) {
        super(message);
    }

    public WorthlessDBException(String message, Throwable cause) {
        super(message, cause);
    }

    public WorthlessDBException(Throwable cause) {
        super(cause);
    }

}
