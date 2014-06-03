package org.kd.worthlessdb.operations;

import org.json.JSONObject;
import org.kd.worthlessdb.WorthlessDBException;

/**
 * @author kirk
 */
public interface Operation<T> {
    T execute(JSONObject arg) throws WorthlessDBException;
}
