package org.kd.worthlessdb.operations.search;

import org.json.JSONObject;

/**
 * @author kirk
 */
public interface SearchOperator {

    boolean match(JSONObject jsonObject);

}
