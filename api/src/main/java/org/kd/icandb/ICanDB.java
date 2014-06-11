package org.kd.icandb;

import java.util.List;
import java.util.Map;

import static org.kd.icandb.ICanDBConstants.*;

/**
 * @author kirk
 */
public interface ICanDB {

    String insert(
            @ReqParam(PARAM_COLLECTION) String collection,
            @ReqParam(PARAM_OBJ) Map<String, ?> obj) throws ICanDBException;

    List<Map<String, ?>> find(
            @ReqParam(PARAM_COLLECTION) String collection,
            @ReqParam(PARAM_QUERY) Map<String, ?> query,
            @ReqParam(PARAM_FIELDS) Map<String, ?> fields) throws ICanDBException;

    default List<Map<String, ?>> find(
            @ReqParam(PARAM_COLLECTION) String collection,
            @ReqParam(PARAM_QUERY) Map<String, ?> query) throws ICanDBException {
        return find(collection, query, null);
    }

    int update(
            @ReqParam(PARAM_COLLECTION) String collection,
            @ReqParam(PARAM_QUERY) Map<String, ?> query,
            @ReqParam(PARAM_OBJ) Map<String, ?> obj) throws ICanDBException;

    int delete(
            @ReqParam(PARAM_COLLECTION) String collection,
            @ReqParam(PARAM_QUERY) Map<String, ?> query) throws ICanDBException;
}
