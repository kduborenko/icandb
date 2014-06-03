package org.kd.worthlessdb.operations;

import org.json.JSONObject;
import org.springframework.stereotype.Component;

/**
 * @author kirk
 */
@Component("echo")
public class EchoOperation implements Operation<JSONObject> {

    @Override
    public JSONObject execute(JSONObject arg) {
        return arg;
    }

}
