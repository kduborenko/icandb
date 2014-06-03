package org.kd.worthlessdb.operations;

import org.json.JSONObject;
import org.springframework.stereotype.Component;

/**
 * @author kirk
 */
@Component("echo")
public class EchoOperation implements Operation {

    @Override
    public Object execute(JSONObject arg) {
        return arg;
    }

}
