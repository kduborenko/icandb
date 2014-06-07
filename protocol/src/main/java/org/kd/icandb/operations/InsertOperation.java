package org.kd.icandb.operations;

import org.json.JSONObject;
import org.kd.icandb.ICanDBException;
import org.springframework.stereotype.Component;

import static org.kd.icandb.ICanDBConstants.PARAM_COLLECTION;
import static org.kd.icandb.ICanDBConstants.PARAM_OBJ;

/**
 * @author kirk
 */
@Component("insert")
public class InsertOperation extends StorageOperation<String> {

    @Override
    public String execute(JSONObject arg) throws ICanDBException {
        String collection = arg.getString(PARAM_COLLECTION);
        return getStorage().insert(collection, arg.getJSONObject(PARAM_OBJ));
    }

}
