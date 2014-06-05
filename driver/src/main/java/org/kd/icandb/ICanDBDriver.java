package org.kd.icandb;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author kirk
 */
public final class ICanDBDriver {

    private static final Pattern CONNECTION_STRING_PROTOCOL_PATTERN
            = Pattern.compile("^(?<protocol>\\w+)://(?<parametersString>.*)$");

    private static final Map<String, Function<String, ICanDB>> DRIVER_BUILDERS
            = Collections.unmodifiableMap(new HashMap<String, Function<String, ICanDB>>() {
        {
            ServiceLoader<ICanDBDriverBuilder> load = ServiceLoader.load(ICanDBDriverBuilder.class);
            load.forEach(b -> put(b.getProtocol(), b::getDriver));
        }
    });

    private ICanDBDriver() {}

    public static ICanDB getDriver(String connectionString) {
        Matcher matcher = CONNECTION_STRING_PROTOCOL_PATTERN.matcher(connectionString);

        if (!matcher.find()) {
            throw new IllegalArgumentException(String.format(
                    "Connection string '%s' cannot be parsed.", connectionString));
        }

        String protocol = matcher.group("protocol");
        if (!DRIVER_BUILDERS.containsKey(protocol)) {
            throw new IllegalArgumentException(String.format(
                    "Protocol '%s' is not supported.", protocol));
        }

        return DRIVER_BUILDERS.get(protocol).apply(matcher.group("parametersString"));
    }

}
