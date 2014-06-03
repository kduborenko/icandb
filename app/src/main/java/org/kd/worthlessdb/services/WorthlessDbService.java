package org.kd.worthlessdb.services;

import org.kd.worthlessdb.WorthlessDBException;

/**
 * @author kirk
 */
public interface WorthlessDbService {
    void start() throws WorthlessDBException;

    void stop();
}
