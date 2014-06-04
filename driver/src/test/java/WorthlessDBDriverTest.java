import org.json.JSONObject;
import org.junit.Ignore;
import org.junit.Test;
import org.kd.icandb.WorthlessDBException;

import static org.kd.icandb.WorthlessDBDriver.getDriver;

/**
 * @author kirk
 */
public class WorthlessDBDriverTest {

    @Ignore
    @Test
    public void networkDriver() throws WorthlessDBException {
        getDriver("").insert("user", new JSONObject().put("name", "Name"));
    }

}
