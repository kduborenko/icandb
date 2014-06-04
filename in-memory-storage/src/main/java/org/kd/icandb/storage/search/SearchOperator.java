package org.kd.icandb.storage.search;

import org.json.JSONObject;

/**
 * @author kirk
 */
public interface SearchOperator {

    boolean match(JSONObject jsonObject);

}
