package org.kd.worthlessdb.storage;

import org.json.JSONObject;
import org.springframework.stereotype.Repository;

import java.util.*;

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
    public void insert(String collection, JSONObject obj) {
        collections.get(collection).put(UUID.randomUUID(), obj);
    }
}
