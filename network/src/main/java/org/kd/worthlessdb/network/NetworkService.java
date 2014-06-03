package org.kd.worthlessdb.network;

import org.kd.worthlessdb.WorthlessDBException;

/**
 * @author kirk
 */
public interface NetworkService {
    void start() throws WorthlessDBException;

    void stop();
}
