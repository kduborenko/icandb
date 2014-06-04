package org.kd.icandb.operations;

import org.json.JSONObject;
import org.kd.icandb.WorthlessDB;
import org.kd.icandb.WorthlessDBException;
import org.springframework.stereotype.Component;

/**
 * @author kirk
 */
@Component("insert")
public class InsertOperation extends StorageOperation<String> {

    @Override
    public String execute(JSONObject arg) throws WorthlessDBException {
        String collection = arg.getString(WorthlessDB.PARAM_COLLECTION);
        return getStorage().insert(collection, arg.getJSONObject(WorthlessDB.PARAM_OBJ));
    }

}
