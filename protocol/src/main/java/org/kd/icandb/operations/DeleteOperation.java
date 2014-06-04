package org.kd.icandb.operations;

import org.json.JSONObject;
import org.kd.icandb.ICanDB;
import org.kd.icandb.ICanDBException;
import org.springframework.stereotype.Component;

/**
 * @author kirk
 */
@Component("delete")
public class DeleteOperation extends StorageOperation<Integer> {
    @Override
    public Integer execute(JSONObject arg) throws ICanDBException {
        String collection = arg.getString(ICanDB.PARAM_COLLECTION);
        return getStorage().delete(collection, arg.getJSONObject(ICanDB.PARAM_QUERY));
    }
}
