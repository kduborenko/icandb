package org.kd.icandb;

import org.json.JSONArray;
import org.json.JSONObject;

/**
 * @author kirk
 */
public interface ICanDB {

    String PARAM_COLLECTION = "$collection";
    String PARAM_OBJ = "$obj";
    String PARAM_QUERY = "$query";
    String PARAM_FIELDS = "$fields";

    String insert(
            @ReqParam(PARAM_COLLECTION) String collection,
            @ReqParam(PARAM_OBJ) JSONObject obj) throws ICanDBException;

    JSONArray find(
            @ReqParam(PARAM_COLLECTION) String collection,
            @ReqParam(PARAM_QUERY) JSONObject query,
            @ReqParam(PARAM_FIELDS) JSONObject fields) throws ICanDBException;

    int update(
            @ReqParam(PARAM_COLLECTION) String collection,
            @ReqParam(PARAM_QUERY) JSONObject query,
            @ReqParam(PARAM_OBJ) JSONObject obj) throws ICanDBException;

    int delete(
            @ReqParam(PARAM_COLLECTION) String collection,
            @ReqParam(PARAM_QUERY) JSONObject query) throws ICanDBException;
}
