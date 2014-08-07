package org.kd.icandb.utils;

import java.util.Collections;
import java.util.Map;

/**
 * @author Kiryl Dubarenka
 */
public final class MapUtils {

    private MapUtils() {}

    public static  <K, V> V get(Map<K, ?> map, K key, Class<V> type) {
        return get(map, key, type, null);
    }

    public static  <K, V> V get(Map<K, ?> map, K key, Class<V> type, V def) {
        return map.containsKey(key) ? type.cast(map.get(key)) : def;
    }

    @SuppressWarnings("unchecked")
    public static  <K, MK, MV> Map<MK, MV> getMap(Map<K, ?> map, K key,
                                                  Class<MK> keyType, Class<MV> valueType) {
        return map.containsKey(key)
                ? Collections.checkedMap((Map<MK, MV>) map.get(key), keyType, valueType)
                : null;
    }

}
