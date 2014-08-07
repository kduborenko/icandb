package org.kd.icandb.operations;

import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * @author Kiryl Dubarenka
 */
@Component("echo")
public class EchoOperation implements Operation<Map<String, ?>> {

    @Override
    public Map<String, ?> execute(Map<String, ?> arg) {
        return arg;
    }

}
