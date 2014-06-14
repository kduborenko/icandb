package org.kd.icandb.storage;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.kd.icandb.ICanDBException;
import org.kd.icandb.ICanDB;
import org.springframework.stereotype.Repository;

import java.util.*;
import java.util.function.BiFunction;
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
        return withIndex(colName, query, (indexResult, scanQuery) -> {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug(String.format("Search in collection '%s'...", colName));
                        LOG.debug(String.format("Query: %s", scanQuery));
                    }
                    List<Map<String, ?>> result = indexResult.stream()
                            .filter(buildSearchOperator(scanQuery)::match)
                            .map(buildFieldsSelector(fields)::map)
                            .collect(Collectors.toList());
                    if (LOG.isDebugEnabled()) {
                        LOG.debug(String.format("Scanned %s object(s), found %s object(s).",
                                indexResult.size(), result.size()));
                    }
                    return result;
                }
        );

    }

    private List<Map<String, ?>> withIndex(String colName, Map<String, ?> query,
            BiFunction<Collection<Map<String, ?>>, Map<String, ?> , List<Map<String, ?>>> callback) {
        for (Index index : indexes.get(colName)) {
            Map<String, ?> indexQuery = index.match(query);
            if (indexQuery != null) {
                List<Map<String, ?>> result = index.find(indexQuery);
                if (result != null) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug(String.format("Search in index '%s', found %s object(s).", index, result.size()));
                        LOG.debug(String.format("Index Query: %s", indexQuery));
                    }
                    @SuppressWarnings("unchecked") Map<String, Object> scanQuery
                            = ((Map<String, Object>) query).entrySet()
                            .stream()
                            .filter(e -> !indexQuery.containsKey(e.getKey()))
                            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
                    return callback.apply(result, scanQuery);
                }
            }
        }
        return callback.apply(collections.get(colName).values(), query);
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

        @Override
        public String toString() {
            return "By ID index";
        }
    }
}
