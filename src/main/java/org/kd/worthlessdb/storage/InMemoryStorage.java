package org.kd.worthlessdb.storage;

import org.json.JSONObject;
import org.kd.worthlessdb.operations.search.SearchOperator;
import org.kd.worthlessdb.operations.search.SearchUtils;
import org.springframework.stereotype.Repository;

import java.util.*;
import java.util.stream.Collectors;

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
        UUID id = UUID.randomUUID();
        collections.get(collection).put(id, obj);
        return id.toString();
    }

    @Override
    public List<JSONObject> find(String collection, JSONObject query) {
        SearchOperator searchOperator = SearchUtils.buildSearchOperator(query);
        return collections.get(collection).values().stream()
                .filter(searchOperator::match)
                .collect(Collectors.toList());
    }
}
