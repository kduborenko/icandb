package org.kd.worthlessdb.storage;

import org.json.JSONObject;

import java.util.List;

/**
 * @author kirk
 */
public interface Storage {

    String insert(String collection, JSONObject obj) throws StorageException;

    List<JSONObject> find(String collection, JSONObject query, JSONObject fields);

    void removeAll();

    int update(String collection, JSONObject query, JSONObject obj);

    int delete(String collection, JSONObject query);
}
