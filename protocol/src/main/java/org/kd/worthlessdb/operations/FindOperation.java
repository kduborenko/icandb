package org.kd.worthlessdb.operations;

import org.json.JSONArray;
import org.json.JSONObject;
import org.kd.worthlessdb.WorthlessDB;
import org.kd.worthlessdb.WorthlessDBException;
import org.springframework.stereotype.Component;

/**
 * @author kirk
 */
@Component("find")
public class FindOperation extends StorageOperation<JSONArray> {
    @Override
    public JSONArray execute(JSONObject arg) throws WorthlessDBException {
        String collection = arg.getString(WorthlessDB.PARAM_COLLECTION);
        return getStorage().find(collection,
                arg.getJSONObject(WorthlessDB.PARAM_QUERY),
                arg.optJSONObject(WorthlessDB.PARAM_FIELDS));
    }
}
