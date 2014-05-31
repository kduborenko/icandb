package org.kd.worthlessdb

import groovy.json.JsonBuilder
import groovy.json.JsonSlurper
import org.junit.AfterClass
import org.junit.BeforeClass
import org.junit.Test
import org.kd.worthlessdb.services.WorthlessDbService
import org.springframework.context.annotation.AnnotationConfigApplicationContext

import static org.junit.Assert.assertEquals

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
        assertEquals([$status: 'ok', $res: [key1: 'val1', key2: 'val2']], parse(br.readLine()));
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

        def result = parse br.readLine()
        result.$res.remove '$id'  // remove generated id
        assertEquals([$status: 'ok', $res: [$inserted: 1]], result);
    }

    private static def json(Closure closure) {
        def builder = new JsonBuilder()
        builder closure
        builder as String
    }

    private static def parse(String json) {
        new JsonSlurper().parseText json
    }
}
