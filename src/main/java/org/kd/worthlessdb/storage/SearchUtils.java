package org.kd.worthlessdb.storage;

import org.json.JSONObject;
import org.kd.worthlessdb.storage.filter.AllFieldsSelector;
import org.kd.worthlessdb.storage.filter.CompoundFieldsSelector;
import org.kd.worthlessdb.storage.filter.FieldsSelector;
import org.kd.worthlessdb.storage.filter.OneFieldSelector;
import org.kd.worthlessdb.storage.search.FieldMatchOperator;
import org.kd.worthlessdb.storage.search.MatchAllOperator;
import org.kd.worthlessdb.storage.search.SearchOperator;

import java.util.*;

/**
 * @author kirk
 */
public final class SearchUtils {

    private static final Set<Class> PRIMITIVE_TYPES = Collections.unmodifiableSet(new HashSet<>(
            Arrays.asList(String.class, Double.class, Integer.class, Boolean.class,
                    double.class, int.class, boolean.class)
    ));

    private SearchUtils() {}

    @SuppressWarnings("unchecked")
    public static SearchOperator buildSearchOperator(JSONObject query) {
        List<SearchOperator> operatorList = new ArrayList<>();
        for (String key : (Set<String>) query.keySet()) {
            Object value = query.get(key);
            if (isPrimitiveValue(value)) {
                operatorList.add(new FieldMatchOperator(key, value));
            }
        }
        if (operatorList.isEmpty()) {
            return MatchAllOperator.INSTANCE;
        }
        if (operatorList.size() == 1) {
            return operatorList.get(0);
        }
        throw new UnsupportedOperationException("Compound search queries are not supported yet.");
    }

    private static boolean isPrimitiveValue(Object value) {
        return value == null || PRIMITIVE_TYPES.contains(value.getClass());
    }

    @SuppressWarnings("unchecked")
    public static FieldsSelector buildFieldsSelector(JSONObject fieldsSelector) {
        if (fieldsSelector == null || fieldsSelector.keySet().isEmpty()) {
            return AllFieldsSelector.INSTANCE;
        }
        List<FieldsSelector> selectorList = new ArrayList<>();
        for (String key : (Set<String>) fieldsSelector.keySet()) {
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
