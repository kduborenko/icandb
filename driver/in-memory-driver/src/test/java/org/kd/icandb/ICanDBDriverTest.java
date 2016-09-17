package org.kd.icandb;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author Kiryl Dubarenka
 */
public class ICanDBDriverTest {

    @Test
    public void testEmbeddedDriver() throws ICanDBException {
        ICanDB driver = ICanDBDriver.getDriver("mem://");
        Map<String, Object> obj = new HashMap<>();
        obj.put("name", "Name1");
        String id = driver.insert("users", obj);
        List<Map<String, ?>> users = driver.find("users", new HashMap<>(), null);

        Map<String, Object> res = new HashMap<>(obj);
        res.put("_id", id);
        assertEquals(Collections.singletonList(res), users);
    }

}
