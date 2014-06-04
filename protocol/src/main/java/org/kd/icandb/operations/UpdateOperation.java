package org.kd.icandb.operations;

import org.json.JSONObject;
import org.kd.icandb.WorthlessDB;
import org.kd.icandb.WorthlessDBException;
import org.springframework.stereotype.Component;

/**
 * @author kirk
 */
@Component("update")
public class UpdateOperation extends StorageOperation<Integer> {
    @Override
    public Integer execute(JSONObject arg) throws WorthlessDBException {
        String collection = arg.getString(WorthlessDB.PARAM_COLLECTION);
        return getStorage().update(collection,
                arg.getJSONObject(WorthlessDB.PARAM_QUERY),
                arg.getJSONObject(WorthlessDB.PARAM_OBJ));
    }
}
