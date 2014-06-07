package org.kd.icandb.operations;

import org.kd.icandb.ICanDBException;

import java.util.Map;

/**
 * @author kirk
 */
public interface Operation<T> {
    T execute(Map<String, ?> arg) throws ICanDBException;
}
