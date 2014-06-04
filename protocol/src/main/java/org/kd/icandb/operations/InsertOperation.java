package org.kd.icandb.operations;

import org.json.JSONObject;
import org.kd.icandb.ICanDB;
import org.kd.icandb.ICanDBException;
import org.springframework.stereotype.Component;

/**
 * @author kirk
 */
@Component("insert")
public class InsertOperation extends StorageOperation<String> {

    @Override
    public String execute(JSONObject arg) throws ICanDBException {
        String collection = arg.getString(ICanDB.PARAM_COLLECTION);
        return getStorage().insert(collection, arg.getJSONObject(ICanDB.PARAM_OBJ));
    }

}
