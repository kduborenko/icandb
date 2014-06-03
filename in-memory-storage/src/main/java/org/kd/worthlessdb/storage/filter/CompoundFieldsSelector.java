package org.kd.worthlessdb.storage.filter;

import org.json.JSONObject;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;

/**
 * @author kirk
 */
public class CompoundFieldsSelector implements FieldsSelector {

    private final Collection<FieldsSelector> fieldsSelectors;

    public CompoundFieldsSelector(Collection<FieldsSelector> fieldsSelectors) {
        this.fieldsSelectors = Collections.unmodifiableCollection(fieldsSelectors);
    }

    @Override
    public JSONObject map(JSONObject jsonObject) {
        return fieldsSelectors.stream().reduce(
                new JSONObject(), (res, fs) -> merge(res, fs.map(jsonObject)), this::merge);
    }

    @SuppressWarnings("unchecked")
    private JSONObject merge(JSONObject res, JSONObject filtered) {
        for (String key : (Set<String>) filtered.keySet()) {
            res.put(key, filtered.get(key));
        }
        return res;
    }

}
