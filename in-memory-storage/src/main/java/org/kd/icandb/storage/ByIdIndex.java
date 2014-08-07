package org.kd.icandb.storage;

import java.util.*;
import java.util.stream.Collectors;

/**
* @author Kiryl Dubarenka
*/
public class ByIdIndex implements Index {

    private final Map<UUID, Map<String, ?>> collection;

    public ByIdIndex(Map<UUID, Map<String, ?>> collection) {
        this.collection = collection;
    }

    @Override
    public Map<String, ?> match(Map<String, ?> fullQuery) {
        if (fullQuery.containsKey(DBCollection.ID_KEY)) {
            Map<String, Object> indexQuery = new HashMap<>();
            indexQuery.put(DBCollection.ID_KEY, fullQuery.get(DBCollection.ID_KEY));
            return indexQuery;
        } else {
            return null;
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public List<Map<String, ?>> find(Map<String, ?> query) {
        Object v = query.get(DBCollection.ID_KEY);
        if (v instanceof Map) {
            Map m = (Map) v;
            if (m.containsKey("$in")) {
                Object ids = m.get("$in");
                if (ids instanceof List) {
                    return ((List<String>) ids)
                            .stream()
                            .map(id -> collection.get(UUID.fromString(id)))
                            .collect(Collectors.toList());
                }
            }
        } else {
            return Arrays.asList(collection.get(UUID.fromString((String) v)));
        }
        return null;
    }

    @Override
    public String getName() {
        return toString();
    }

    @Override
    public int size() {
        return collection.size();
    }

    @Override
    public String toString() {
        return "By ID index";
    }
}
