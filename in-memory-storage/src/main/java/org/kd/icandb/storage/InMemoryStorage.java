package org.kd.icandb.storage;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONArray;
import org.json.JSONObject;
import org.kd.icandb.WorthlessDBException;
import org.kd.icandb.WorthlessDB;
import org.springframework.stereotype.Repository;

import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.kd.icandb.storage.SearchUtils.buildFieldsSelector;
import static org.kd.icandb.storage.SearchUtils.buildSearchOperator;

/**
 * @author kirk
 */
@Repository
public class InMemoryStorage implements WorthlessDB {

    private static final Log LOG = LogFactory.getLog(InMemoryStorage.class);

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
    public String insert(String colName, JSONObject obj) throws WorthlessDBException {
        String idString = obj.optString("_id", UUID.randomUUID().toString());
        obj.put("_id", idString);
        Map<UUID, JSONObject> collection = collections.get(colName);
        UUID id = UUID.fromString(idString);
        if (collection.containsKey(id)) {
            throw new WorthlessDBException(
                    String.format("Object with id '%s' already exists in collection '%s'.", idString, colName));
        }
        collection.put(id, obj);
        return idString;
    }

    @Override
    public JSONArray find(String collection, JSONObject query, JSONObject fields) {
        return new JSONArray(findObjects(collection, query, fields));
    }

    public List<JSONObject> findObjects(String collection, JSONObject query, JSONObject fields) {
        return collections.get(collection).values().stream()
                .filter(buildSearchOperator(query)::match)
                .map(buildFieldsSelector(fields)::map)
                .collect(Collectors.toList());
    }

    @SuppressWarnings("UnusedDeclaration")
    public void removeAll() {
        new HashSet<>(collections.keySet()).forEach(collections::remove);
    }

    @Override
    public int update(String colName, JSONObject query, JSONObject obj) {
        return findObjects(colName, query, new JSONObject().put("_id", 1))
                .stream()
                .map((rs) -> rs.getString("_id"))
                .map(updateById(colName, obj))
                .reduce(0, Integer::sum);
    }

    private Function<String, Integer> updateById(String colName, JSONObject obj) {
        return handleExceptions(
                (id) -> {
                    collections.get(colName).put(UUID.fromString(id), obj.put("_id", id));
                    return 1;
                },
                0,
                (id) -> LOG.error("Cannot update record by id: " + id)
        );
    }

    @Override
    public int delete(String colName, JSONObject query) {
        return findObjects(colName, query, new JSONObject().put("_id", 1))
                .stream()
                .map((rs) -> rs.getString("_id"))
                .map(deleteById(colName))
                .reduce(0, Integer::sum);
    }

    private Function<String, Integer> deleteById(String colName) {
        return handleExceptions(
                (id) -> collections.get(colName).remove(UUID.fromString(id)) == null ? 0 : 1,
                0,
                (id) -> LOG.error("Cannot delete record by id: " + id)
        );
    }

    private static <T, R> Function<T, R> handleExceptions(Function<T, R> function, R defaultValue, Consumer<T> onError) {
        return (T t) -> {
            try {
                return function.apply(t);
            } catch (Exception e) {
                onError.accept(t);
                return defaultValue;
            }
        };
    }
}
