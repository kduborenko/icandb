import org.json.JSONObject;
import org.junit.Ignore;
import org.junit.Test;
import org.kd.worthlessdb.WorthlessDBException;

import static org.kd.worthlessdb.WorthlessDBDriver.getDriver;

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
