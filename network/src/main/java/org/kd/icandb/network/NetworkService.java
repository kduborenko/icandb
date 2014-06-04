package org.kd.icandb.network;

import org.kd.icandb.WorthlessDBException;

/**
 * @author kirk
 */
public interface NetworkService {
    void start() throws WorthlessDBException;

    void stop();
}
