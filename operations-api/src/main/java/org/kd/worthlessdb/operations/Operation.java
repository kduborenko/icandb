package org.kd.worthlessdb.operations;

import org.json.JSONObject;
import org.kd.worthlessdb.WorthlessDBException;

/**
 * @author kirk
 */
public interface Operation {
    Object execute(JSONObject arg) throws WorthlessDBException;
}
