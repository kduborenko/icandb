package org.kd.icandb.storage.selectors;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Kiryl Dubarenka
 */
public class CompoundFieldsSelector implements FieldsSelector {

    private final Collection<FieldsSelector> fieldsSelectors;

    public CompoundFieldsSelector(Collection<FieldsSelector> fieldsSelectors) {
        this.fieldsSelectors = Collections.unmodifiableCollection(fieldsSelectors);
    }

    @Override
    public Map<String, ?> map(Map<String, ?> jsonObject) {
        return fieldsSelectors.stream().reduce(
                new HashMap<>(), (res, fs) -> merge(res, fs.map(jsonObject)), this::merge);
    }

    @SuppressWarnings("unchecked")
    private Map<String, ?> merge(Map<String, ?> m1, Map<String, ?> m2) {
        Map<String, Object> res = new HashMap<>();
        res.putAll(m1);
        res.putAll(m2);
        return res;
    }

}
