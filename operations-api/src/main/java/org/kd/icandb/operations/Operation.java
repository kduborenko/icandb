package org.kd.icandb.operations;

import org.json.JSONObject;
import org.kd.icandb.WorthlessDBException;

/**
 * @author kirk
 */
public interface Operation<T> {
    T execute(JSONObject arg) throws WorthlessDBException;
}
