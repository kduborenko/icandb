package org.kd.icandb.operations;

import org.kd.icandb.ICanDBException;
import org.springframework.stereotype.Component;

import java.util.Map;

import static org.kd.icandb.ICanDBConstants.PARAM_COLLECTION;
import static org.kd.icandb.ICanDBConstants.PARAM_OBJ;
import static org.kd.icandb.utils.MapUtils.get;
import static org.kd.icandb.utils.MapUtils.getMap;

/**
 * @author Kiryl Dubarenka
 */
@Component("insert")
public class InsertOperation extends StorageOperation<String> {

    @Override
    public String execute(Map<String, ?> arg) throws ICanDBException {
        String collection = get(arg, PARAM_COLLECTION, String.class);
        return getStorage().insert(collection, getMap(arg, PARAM_OBJ, String.class, Object.class));
    }

}
