package org.kd.worthlessdb;

import org.json.JSONObject;

import java.util.List;

/**
 * @author kirk
 */
public interface WorthlessDB {

    String insert(String collection, JSONObject obj) throws WorthlessDBException;

    List<JSONObject> find(String collection, JSONObject query, JSONObject fields);

    int update(String collection, JSONObject query, JSONObject obj);

    int delete(String collection, JSONObject query);
}
