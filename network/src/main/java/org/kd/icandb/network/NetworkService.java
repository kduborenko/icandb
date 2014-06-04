package org.kd.icandb.network;

import org.kd.icandb.ICanDBException;

/**
 * @author kirk
 */
public interface NetworkService {
    void start() throws ICanDBException;

    void stop();
}
