package org.kd.icandb.network;

import org.kd.icandb.ICanDBException;

/**
 * @author Kiryl Dubarenka
 */
public interface NetworkService {
    void start() throws ICanDBException;

    void stop();
}
