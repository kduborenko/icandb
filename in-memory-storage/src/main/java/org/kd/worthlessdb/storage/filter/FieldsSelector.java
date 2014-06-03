package org.kd.worthlessdb.storage.filter;

import org.json.JSONObject;

/**
 * @author kirk
 */
public interface FieldsSelector {

    JSONObject map(JSONObject jsonObject);

}
