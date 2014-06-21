package org.kd.icandb.storage;

import java.util.Collection;
import java.util.Map;
import java.util.UUID;

/**
 * @author kirk
 */
public interface DBCollection {
    String ID_KEY = "_id";

    boolean isExists(UUID id);

    void put(UUID id, Map<String, ?> obj);

    Collection<? extends Index> getIndexes();

    Collection<Map<String,?>> values();

    Map<String, ?> remove(UUID id);
}
