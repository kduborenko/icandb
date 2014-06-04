package org.kd.icandb.operations;

import org.json.JSONObject;
import org.kd.icandb.ICanDB;
import org.kd.icandb.WorthlessDBException;
import org.springframework.stereotype.Component;

/**
 * @author kirk
 */
@Component("update")
public class UpdateOperation extends StorageOperation<Integer> {
    @Override
    public Integer execute(JSONObject arg) throws WorthlessDBException {
        String collection = arg.getString(ICanDB.PARAM_COLLECTION);
        return getStorage().update(collection,
                arg.getJSONObject(ICanDB.PARAM_QUERY),
                arg.getJSONObject(ICanDB.PARAM_OBJ));
    }
}
