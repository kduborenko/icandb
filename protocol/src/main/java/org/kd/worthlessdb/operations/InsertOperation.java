package org.kd.worthlessdb.operations;

import org.json.JSONObject;
import org.kd.worthlessdb.WorthlessDB;
import org.kd.worthlessdb.WorthlessDBException;
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
