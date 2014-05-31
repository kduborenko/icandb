package org.kd.worthlessdb.storage;

import org.json.JSONObject;

/**
 * @author kirk
 */
public interface Storage {

    String insert(String collection, JSONObject obj);

}
