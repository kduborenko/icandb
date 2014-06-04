package org.kd.icandb.services;

import org.kd.icandb.WorthlessDBException;

/**
 * @author kirk
 */
public interface WorthlessDbService {
    void start() throws WorthlessDBException;

    void stop();
}
