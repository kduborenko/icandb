package org.kd.worthlessdb.storage;

import org.json.JSONObject;

/**
 * @author kirk
 */
public interface Storage {

    void insert(String collection, JSONObject obj);

}
