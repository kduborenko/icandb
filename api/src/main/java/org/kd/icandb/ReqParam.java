package org.kd.icandb;

import java.lang.annotation.*;

/**
 * @author kirk
 */
@Target({ElementType.PARAMETER})
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface ReqParam {
    String value();
}
