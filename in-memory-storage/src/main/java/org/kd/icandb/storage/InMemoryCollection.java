package org.kd.icandb.storage;

import java.util.*;

/**
 * @author kirk
 */
public class InMemoryCollection implements DBCollection {

    private final Map<UUID, Map<String, ?>> objects;

    private final List<Index> indexes;

    public InMemoryCollection() {
        this.objects = new HashMap<>();
        this.indexes = new ArrayList<>(Arrays.asList(new ByIdIndex(objects)));
    }

    @Override
    public boolean isExists(UUID id) {
        return objects.containsKey(id);
    }

    @Override
    public void put(UUID id, Map<String, ?> obj) {
        objects.put(id, obj);
    }

    @Override
    public Collection<? extends Index> getIndexes() {
        return Collections.unmodifiableCollection(indexes);
    }

    @Override
    public Collection<Map<String, ?>> values() {
        return Collections.unmodifiableCollection(objects.values());
    }

    @Override
    public Map<String, ?> remove(UUID id) {
        return objects.remove(id);
    }
}
