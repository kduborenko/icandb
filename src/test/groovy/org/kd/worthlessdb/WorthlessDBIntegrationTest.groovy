package org.kd.worthlessdb

import groovy.json.JsonBuilder
import org.json.JSONStringer
import org.junit.AfterClass
import org.junit.Assert
import org.junit.BeforeClass
import org.junit.Test
import org.kd.worthlessdb.services.WorthlessDbService
import org.springframework.context.annotation.AnnotationConfigApplicationContext

/**
 * @author kirk
 */
class WorthlessDBIntegrationTest {

    private static final def context = new AnnotationConfigApplicationContext("org.kd.worthlessdb");

    @BeforeClass
    public static void setUp() {
        context.getBean(WorthlessDbService.class).start();
    }

    @AfterClass
    public static void tearDown() {
        context.getBean(WorthlessDbService.class).stop();
    }

    @Test
    public void echo() throws IOException {
        Socket socket = new Socket("localhost", 8978);
        BufferedReader br = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        PrintWriter pw = new PrintWriter(socket.getOutputStream(), true);
        String command = json {
            $op 'echo'
            $arg {
                key1 'val1'
                key2 'val2'
            }
        };
        pw.println(command);
        Assert.assertEquals(json {
            $status 'ok'
            $res {
                key1 'val1'
                key2 'val2'
            }
        }, br.readLine());
    }

    @Test
    public void insert() throws IOException {
        Socket socket = new Socket("localhost", 8978);
        BufferedReader br = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        PrintWriter pw = new PrintWriter(socket.getOutputStream(), true);
        String command = json {
            $op 'insert'
            $arg {
                $collection 'users'
                $obj {
                    name 'Name'
                    age 26
                }
            }
        };
        pw.println(command);
        Assert.assertEquals(json {
            $status 'ok'
            $res {
                $inserted 1
            }
        }, br.readLine());
    }

    private static def json(Closure closure) {
        def builder = new JsonBuilder()
        builder closure
        builder as String
    }
}
