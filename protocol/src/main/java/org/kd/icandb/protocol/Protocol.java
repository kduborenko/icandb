package org.kd.icandb.protocol;

import java.io.InputStream;
import java.io.OutputStream;

/**
 * @author kirk
 */
public interface Protocol {

    void handleInputStream(InputStream inputStream, OutputStream outputStream);

}
