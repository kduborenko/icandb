package org.kd.worthlessdb.storage;

import org.json.JSONObject;

import java.util.List;

/**
 * @author kirk
 */
public interface Storage {

    String insert(String collection, JSONObject obj);

    List<JSONObject> find(String collection, JSONObject query);
}
