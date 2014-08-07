package org.kd.icandb.services;

import org.kd.icandb.ICanDBException;

/**
 * @author Kiryl Dubarenka
 */
public interface ICanDBService {
    void start() throws ICanDBException;

    void stop();
}
