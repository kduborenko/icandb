package org.kd.icandb.services;

import org.kd.icandb.ICanDBException;

/**
 * @author kirk
 */
public interface WorthlessDbService {
    void start() throws ICanDBException;

    void stop();
}
