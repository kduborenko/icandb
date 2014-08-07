package org.kd.icandb.operations;

import org.kd.icandb.ICanDBException;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

import static org.kd.icandb.ICanDBConstants.*;
import static org.kd.icandb.utils.MapUtils.get;
import static org.kd.icandb.utils.MapUtils.getMap;

/**
 * @author Kiryl Dubarenka
 */
@Component("find")
public class FindOperation extends StorageOperation<List<?>> {
    @Override
    public List<?> execute(Map<String, ?> arg) throws ICanDBException {
        String collection = get(arg, PARAM_COLLECTION, String.class);
        return getStorage().find(collection,
                getMap(arg, PARAM_QUERY, String.class, Object.class),
                getMap(arg, PARAM_FIELDS, String.class, Object.class));
    }
}
