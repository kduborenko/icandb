package org.kd.worthlessdb.operations;

import org.json.JSONObject;

/**
 * @author kirk
 */
public interface Operation {
    JSONObject execute(JSONObject arg);
}
