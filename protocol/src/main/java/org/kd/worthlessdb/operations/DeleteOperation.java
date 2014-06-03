package org.kd.worthlessdb.operations;

import org.json.JSONObject;
import org.kd.worthlessdb.WorthlessDB;
import org.kd.worthlessdb.WorthlessDBException;
import org.springframework.stereotype.Component;

/**
 * @author kirk
 */
@Component("delete")
public class DeleteOperation extends StorageOperation<Integer> {
    @Override
    public Integer execute(JSONObject arg) throws WorthlessDBException {
        String collection = arg.getString(WorthlessDB.PARAM_COLLECTION);
        return getStorage().delete(collection, arg.getJSONObject(WorthlessDB.PARAM_QUERY));
    }
}
