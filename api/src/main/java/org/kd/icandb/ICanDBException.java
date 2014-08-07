package org.kd.icandb;

/**
 * @author Kiryl Dubarenka
 */
public class ICanDBException extends Exception {

    public ICanDBException() {}

    public ICanDBException(String message) {
        super(message);
    }

    public ICanDBException(String message, Throwable cause) {
        super(message, cause);
    }

    public ICanDBException(Throwable cause) {
        super(cause);
    }

}
