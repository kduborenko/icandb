package org.kd.icandb;

/**
 * @author kirk
 */
public interface ICanDBDriverBuilder {

    String getProtocol();

    ICanDB getDriver(String connectionString);

}
