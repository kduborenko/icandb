package org.kd.icandb.operations;

import org.kd.icandb.ICanDBException;
import org.springframework.stereotype.Component;

import java.util.Map;

import static org.kd.icandb.ICanDBConstants.PARAM_COLLECTION;
import static org.kd.icandb.ICanDBConstants.PARAM_QUERY;
import static org.kd.icandb.utils.MapUtils.get;
import static org.kd.icandb.utils.MapUtils.getMap;

/**
 * @author Kiryl Dubarenka
 */
@Component("delete")
public class DeleteOperation extends StorageOperation<Integer> {
    @Override
    public Integer execute(Map<String, ?> arg) throws ICanDBException {
        String collection = get(arg, PARAM_COLLECTION, String.class);
        return getStorage().delete(collection, getMap(arg, PARAM_QUERY, String.class, Object.class));
    }
}
