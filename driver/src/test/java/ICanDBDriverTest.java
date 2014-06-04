import org.json.JSONObject;
import org.junit.Ignore;
import org.junit.Test;
import org.kd.icandb.ICanDBException;

import static org.kd.icandb.ICanDBDriver.getDriver;

/**
 * @author kirk
 */
public class ICanDBDriverTest {

    @Ignore
    @Test
    public void networkDriver() throws ICanDBException {
        getDriver("").insert("user", new JSONObject().put("name", "Name"));
    }

}
