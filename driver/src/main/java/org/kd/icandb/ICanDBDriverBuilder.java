package org.kd.icandb;

/**
 * @author Kiryl Dubarenka
 */
public interface ICanDBDriverBuilder {

    String getProtocol();

    ICanDB getDriver(String connectionString);

}
