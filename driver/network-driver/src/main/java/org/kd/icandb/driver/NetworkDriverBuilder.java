package org.kd.icandb.driver;

import org.kd.icandb.ICanDB;
import org.kd.icandb.ICanDBDriverBuilder;

import java.io.IOException;
import java.net.Socket;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author Kiryl Dubarenka
 */
public class NetworkDriverBuilder implements ICanDBDriverBuilder {

    private static final Pattern CONNECTION_STRING_PATTERN
            = Pattern.compile("^(?<hostname>.+?):(?<port>\\d{1,5})/$");

    @Override
    public String getProtocol() {
        return "net";
    }

    @Override
    public ICanDB getDriver(String connectionString) {
        Matcher matcher = CONNECTION_STRING_PATTERN.matcher(connectionString);

        if (!matcher.find()) {
            throw new IllegalArgumentException(String.format(
                    "Connection string '%s' cannot be parsed.", connectionString));
        }

        return getNetworkDriver(matcher);
    }

    private static ICanDB getNetworkDriver(Matcher matcher) {
        try {
            Socket socket = new Socket(matcher.group("hostname"),
                    Integer.valueOf(matcher.group("port")));
            return new ICanDBNetworkDriver(socket);
        } catch (IOException e) {
            throw new RuntimeException(e); //todo throw better exception
        }
    }
}
