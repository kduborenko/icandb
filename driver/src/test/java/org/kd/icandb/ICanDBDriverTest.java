package org.kd.icandb;

import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * @author kirk
 */
public class ICanDBDriverTest {

    @Test
    public void testEmbeddedDriver() throws ICanDBException {
        ICanDB driver = ICanDBDriver.getDriver("mem://");
        String id = driver.insert("users", new JSONObject().put("name", "Name1"));
        JSONArray users = driver.find("users", new JSONObject(), null);
        assertEquals(1, users.length());
        assertEquals(id, users.getJSONObject(0).getString("_id"));
        assertEquals("Name1", users.getJSONObject(0).getString("name"));
    }

}
