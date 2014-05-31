package org.kd.worthlessdb.storage;

import org.json.JSONObject;
import org.springframework.stereotype.Repository;

import java.util.*;
import java.util.stream.Collectors;

import static org.kd.worthlessdb.storage.search.SearchUtils.buildFieldsSelector;
import static org.kd.worthlessdb.storage.search.SearchUtils.buildSearchOperator;

/**
 * @author kirk
 */
@Repository
public class InMemoryStorage implements Storage {

    private Map<String, Map<UUID, JSONObject>> collections = new HashMap<String, Map<UUID, JSONObject>>() {
        @Override
        public Map<UUID, JSONObject> get(Object key) {
            Map<UUID, JSONObject> collection = super.get(key);
            if (collection == null) {
                super.put((String) key, collection = new HashMap<>());
            }
            return collection;
        }
    };

    @Override
    public String insert(String collection, JSONObject obj) {
        String idString = obj.optString("_id", UUID.randomUUID().toString());
        obj.put("_id", idString);
        collections.get(collection).put(UUID.fromString(idString), obj);
        return idString;
    }

    @Override
    public List<JSONObject> find(String collection, JSONObject query, JSONObject fields) {
        return collections.get(collection).values().stream()
                .filter(buildSearchOperator(query)::match)
                .map(buildFieldsSelector(fields)::map)
                .collect(Collectors.toList());
    }
}
