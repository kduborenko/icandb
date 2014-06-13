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
    private static final String ID_KEY = "_id";

    private static final Map<String, ?> SELECT_ID = Collections.unmodifiableMap(new HashMap<String, Object>() {{
        put(ID_KEY, 1);
    }});

    private Map<String, Map<UUID, Map<String, ?>>> collections = new HashMap<String, Map<UUID, Map<String, ?>>>() {
        @Override
        public Map<UUID, Map<String, ?>> get(Object key) {
            Map<UUID, Map<String, ?>> collection = super.get(key);
            if (collection == null) {
                super.put((String) key, collection = new HashMap<>());
                indexes.put((String) key, new ArrayList<>(Arrays.asList(new ByIdIndex(collection))));
            }
            return collection;
        }
    };

    private Map<String, List<Index>> indexes = new HashMap<>();

    @Override
    public String insert(String colName, Map<String, ?> obj) throws ICanDBException {
        Map<String, Object> o = new HashMap<>(obj);
        String idString = get(o, ID_KEY, String.class, UUID.randomUUID().toString());
        o.put(ID_KEY, idString);
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
    public List<Map<String, ?>> find(String colName, Map<String, ?> query, Map<String, ?> fields)
            throws ICanDBException {
        Collection<Map<String, ?>> collection = tryIndex(colName, query);
        if (query.isEmpty()) {
            return collection instanceof List
                    ? (List<Map<String, ?>>) collection
                    : new ArrayList<>(collection);
        }
        return collection.stream()
                .filter(buildSearchOperator(query)::match)
                .map(buildFieldsSelector(fields)::map)
                .collect(Collectors.toList());
    }

    private Collection<Map<String, ?>> tryIndex(String collection, Map<String, ?> query) {
        for (Index index : indexes.get(collection)) {
            Map<String, ?> indexQuery = index.match(query);
            if (indexQuery != null) {
                List<Map<String, ?>> result = index.find(indexQuery);
                if (result != null) {
                    indexQuery.keySet().forEach(query::remove);
                    return result;
                }
            }
        }
        return collections.get(collection).values();
    }

    @SuppressWarnings("UnusedDeclaration")
    public void removeAll() {
        new HashSet<>(collections.keySet()).forEach(collections::remove);
    }

    @Override
    public int update(String colName, Map<String, ?> query, Map<String, ?> obj) throws ICanDBException {
        return find(colName, query, SELECT_ID)
                .stream()
                .map((rs) -> get(rs, ID_KEY, String.class))
                .map(updateById(colName, obj))
                .reduce(0, Integer::sum);
    }

    private Function<String, Integer> updateById(String colName, Map<String, ?> obj) {
        Map<String, Object> o = new HashMap<>(obj);
        return handleExceptions(
                (id) -> {
                    o.put(ID_KEY, id);
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
                .map((rs) -> get(rs, ID_KEY, String.class))
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

    private class ByIdIndex implements Index {

        private Map<UUID, Map<String, ?>> collection;

        public ByIdIndex(Map<UUID, Map<String, ?>> collection) {
            this.collection = collection;
        }

        @Override
        public Map<String, ?> match(Map<String, ?> fullQuery) {
            if (fullQuery.containsKey(ID_KEY)) {
                Map<String, Object> indexQuery = new HashMap<>();
                indexQuery.put(ID_KEY, fullQuery.get(ID_KEY));
                return indexQuery;
            } else {
                return null;
            }
        }

        @Override
        @SuppressWarnings("unchecked")
        public List<Map<String, ?>> find(Map<String, ?> query) {
            Object v = query.get(ID_KEY);
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
    }
}
