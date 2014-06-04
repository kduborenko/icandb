package org.kd.icandb.operations;

import org.json.JSONArray;
import org.json.JSONObject;
import org.kd.icandb.ICanDB;
import org.kd.icandb.ICanDBException;
import org.springframework.stereotype.Component;

/**
 * @author kirk
 */
@Component("find")
public class FindOperation extends StorageOperation<JSONArray> {
    @Override
    public JSONArray execute(JSONObject arg) throws ICanDBException {
        String collection = arg.getString(ICanDB.PARAM_COLLECTION);
        return getStorage().find(collection,
                arg.getJSONObject(ICanDB.PARAM_QUERY),
                arg.optJSONObject(ICanDB.PARAM_FIELDS));
    }
}
