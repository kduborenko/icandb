package org.kd.icandb.storage;

import org.kd.icandb.storage.search.*;
import org.kd.icandb.storage.selectors.AllFieldsSelector;
import org.kd.icandb.storage.selectors.CompoundFieldsSelector;
import org.kd.icandb.storage.selectors.FieldsSelector;
import org.kd.icandb.storage.selectors.OneFieldSelector;
import org.kd.icandb.utils.MapUtils;
import org.springframework.util.ObjectUtils;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * @author kirk
 */
public final class SearchUtils {

    private SearchUtils() {}

    public static SearchOperator buildSearchOperator(Map<String, ?> query) {
        if (query == null || query.isEmpty()) {
            return MatchAllOperator.INSTANCE;
        }
        List<SearchOperator> operatorList = query.entrySet()
                .stream()
                .map(entry -> {
                    String key = entry.getKey();
                    Object value = entry.getValue();
                    return key.startsWith("$")
                            ? buildSpecialOperator(key, value)
                            : new FieldMatchOperator(key, buildFieldMatcher(value));
                })
                .collect(Collectors.toList());
        return operatorList.size() == 1
                ? operatorList.get(0)
                : new CompoundSearchOperator(operatorList);
    }

    @SuppressWarnings("unchecked")
    private static SearchOperator buildSpecialOperator(String name, Object params) {
        if ("$or".equals(name)) {
            if (!(params instanceof List)) {
                throw new IllegalArgumentException("Parameter for '$or' operator should be list.");
            }
            List<?> list = (List<?>) params;
            if (list.isEmpty()) {
                throw new IllegalArgumentException("Parameter for '$or' operator should be non-empty list.");
            }
            List<SearchOperator> ops = list.stream().map(p -> {
                if (!(p instanceof Map)) {
                    throw new IllegalArgumentException("Parameter for '$or' operator should be list of objects.");
                }
                return buildSearchOperator((Map<String, ?>) p);
            }).collect(Collectors.<SearchOperator>toList());
            return ops.size() == 1
                    ? ops.get(0)
                    : new OrOperator(ops);
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    private static <T> Function<T, Boolean> buildFieldMatcher(T value) {
        if (value instanceof Map){
            Map m = (Map) value;
            if (m.containsKey("$in")) {
                return v -> MapUtils.get(m, "$in", List.class).contains(v);
            } else {
                return v -> ObjectUtils.nullSafeEquals(value, v);
            }
        } else {
            return v -> ObjectUtils.nullSafeEquals(value, v);
        }
    }

    @SuppressWarnings("unchecked")
    public static FieldsSelector buildFieldsSelector(Map<String, ?> fieldsSelector) {
        if (fieldsSelector == null || fieldsSelector.keySet().isEmpty()) {
            return AllFieldsSelector.INSTANCE;
        }
        List<FieldsSelector> selectorList = new ArrayList<>();
        for (String key : fieldsSelector.keySet()) {
            Object value = fieldsSelector.get(key);
            if (Integer.valueOf(1).equals(value)) {
                selectorList.add(new OneFieldSelector(key));
            } else {
                throw new UnsupportedOperationException(
                        String.format("Unsupported field selector definition: %s.", value));
            }
        }
        if (selectorList.size() == 1) {
            return selectorList.get(0);
        }
        return new CompoundFieldsSelector(selectorList);
    }
}
