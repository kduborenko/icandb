package org.kd.icandb.operations;

import org.json.JSONArray;
import org.json.JSONObject;
import org.kd.icandb.ICanDBException;
import org.springframework.stereotype.Component;

import static org.kd.icandb.ICanDBConstants.*;

/**
 * @author kirk
 */
@Component("find")
public class FindOperation extends StorageOperation<JSONArray> {
    @Override
    public JSONArray execute(JSONObject arg) throws ICanDBException {
        String collection = arg.getString(PARAM_COLLECTION);
        return getStorage().find(collection,
                arg.getJSONObject(PARAM_QUERY),
                arg.optJSONObject(PARAM_FIELDS));
    }
}
