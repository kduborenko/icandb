package org.kd.icandb

import org.codehaus.groovy.tools.shell.Groovysh
import org.codehaus.groovy.tools.shell.IO
import org.codehaus.groovy.tools.shell.util.Logger
import org.codehaus.groovy.tools.shell.util.NoExitSecurityManager

/**
 * @author kirk
 */
class ICanDBShell {

    static void main(String[] args) {
        IO io = new IO()
        Logger.io = io

        def db = ICanDBDriver.getDriver("mem://")

        final Groovysh shell = new Groovysh(
                new Binding([db:db]), io)

        SecurityManager sm = System.securityManager
        System.securityManager = new NoExitSecurityManager()

        int code = 0

        try {
            code = shell.run()
        }
        finally {
            System.securityManager = sm
        }

        System.exit(code)
    }

}
