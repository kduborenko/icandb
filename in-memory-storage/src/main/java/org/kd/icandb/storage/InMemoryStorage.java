package org.kd.icandb.storage;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.kd.icandb.ICanDBException;
import org.kd.icandb.ICanDB;
import org.springframework.stereotype.Repository;

import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.kd.icandb.storage.SearchUtils.buildFieldsSelector;
import static org.kd.icandb.storage.SearchUtils.buildSearchOperator;
import static org.kd.icandb.utils.MapUtils.get;

/**
 * @author kirk
 */
@Repository
public class InMemoryStorage implements ICanDB {

    private static final Log LOG = LogFactory.getLog(InMemoryStorage.class);

    private static final Map<String, ?> SELECT_ID = Collections.unmodifiableMap(new HashMap<String, Object>() {{
        put("_id", 1);
    }});

    private Map<String, Map<UUID, Map<String, ?>>> collections = new HashMap<String, Map<UUID, Map<String, ?>>>() {
        @Override
        public Map<UUID, Map<String, ?>> get(Object key) {
            Map<UUID, Map<String, ?>> collection = super.get(key);
            if (collection == null) {
                super.put((String) key, collection = new HashMap<>());
            }
            return collection;
        }
    };

    @Override
    public String insert(String colName, Map<String, ?> obj) throws ICanDBException {
        Map<String, Object> o = new HashMap<>(obj);
        String idString = get(o, "_id", String.class, UUID.randomUUID().toString());
        o.put("_id", idString);
        Map<UUID, Map<String, ?>> collection = collections.get(colName);
        UUID id = UUID.fromString(idString);
        if (collection.containsKey(id)) {
            throw new ICanDBException(
                    String.format("Object with id '%s' already exists in collection '%s'.", idString, colName));
        }
        collection.put(id, o);
        return idString;
    }

    @Override
    public List<Map<String, ?>> find(String collection, Map<String, ?> query, Map<String, ?> fields)
            throws ICanDBException {
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
    public int update(String colName, Map<String, ?> query, Map<String, ?> obj) throws ICanDBException {
        return find(colName, query, SELECT_ID)
                .stream()
                .map((rs) -> get(rs, "_id", String.class))
                .map(updateById(colName, obj))
                .reduce(0, Integer::sum);
    }

    private Function<String, Integer> updateById(String colName, Map<String, ?> obj) {
        Map<String, Object> o = new HashMap<>(obj);
        return handleExceptions(
                (id) -> {
                    o.put("_id", id);
                    collections.get(colName).put(UUID.fromString(id), o);
                    return 1;
                },
                0,
                (id) -> LOG.error("Cannot update record by id: " + id)
        );
    }

    @Override
    public int delete(String colName, Map<String, ?> query) throws ICanDBException {
        return find(colName, query, SELECT_ID)
                .stream()
                .map((rs) -> get(rs, "_id", String.class))
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
