package org.kd.icandb

import groovy.json.JsonBuilder
import groovy.json.JsonSlurper
import org.json.JSONObject
import org.junit.After
import org.junit.AfterClass
import org.junit.BeforeClass
import org.junit.Test
import org.kd.icandb.services.ICanDBService
import org.springframework.context.annotation.AnnotationConfigApplicationContext

import static org.junit.Assert.assertEquals

/**
 * @author kirk
 */
class ICanDBIntegrationTest {

    private static final def context = new AnnotationConfigApplicationContext("org.kd.icandb");
    private static ICanDB driver;

    @BeforeClass
    public static void setUp() {
        context.getBean(ICanDBService).start()
        driver = WorthlessDBDriver.getDriver("net://localhost:8978/")
    }

    @AfterClass
    public static void tearDown() {
        context.getBean(ICanDBService).stop()
        driver == null  // todo close driver
    }

    @After
    public void clean() {
        context.getBean(ICanDB).removeAll()
    }

    @Test
    public void echo() {
        Socket socket = new Socket("localhost", 8978);
        BufferedReader br = new BufferedReader(new InputStreamReader(socket.inputStream));
        PrintWriter pw = new PrintWriter(socket.outputStream, true);
        String command = json {
            $op 'echo'
            $arg {
                key1 'val1'
                key2 'val2'
            }
        };
        pw.println(command);
        assertEquals([$status: 'ok', $res: [key1: 'val1', key2: 'val2']],
                new JsonSlurper().parseText(br.readLine()));
    }

    @Test
    public void insert() {
        driver.insert 'users', json {
            name 'Name1'
            age 26
        }
    }

    @Test(expected = ICanDBException)
    public void insertUsingExistingID() {
        def id = driver.insert 'users', json {
            name 'Name1'
            age 26
        }
        driver.insert 'users', json {
            _id id
            name 'Name2'
            age 27
        }
    }

    @Test
    public void update() {
        def id = driver.insert 'users', json {
            name 'Name1'
            age 26
        }
        def updated = driver.update 'users', json {
            _id id
        }, json {
            name 'Name2'
            age 27
        }

        assertEquals(1, updated);


        def rs = driver.find('users', new JSONObject(), null).get(0) as JSONObject
        assertEquals(id, rs._id);
        assertEquals('Name2', rs.name);
        assertEquals(27, rs.age);
    }

    @Test
    public void delete() {
        driver.insert 'users', json {
            name 'Name1'
            age 26
        }
        def id = driver.insert 'users', json {
            name 'Name2'
            age 27
        }
        def deleted = driver.delete 'users', json {
            name 'Name1'
        }
        assertEquals(1, deleted);

        def rs = driver.find('users', new JSONObject(), null).get(0) as JSONObject
        assertEquals(id, rs._id);
        assertEquals('Name2', rs.name);
        assertEquals(27, rs.age)
    }
    
    @Test
    public void findByField() {
        def id1 = driver.insert 'users', json {
            name 'Name1'
            age 26
        }
        driver.insert 'users', json {
            name 'Name2'
            age 27
        }
        def id3 = driver.insert 'users', json {
            name 'Name3'
            age 26
        }

        def res = driver.find 'users', json {
            age 26
        }, null
        assertEquals([id1, id3] as Set, [res.get(0)._id, res.get(1)._id] as Set);
    }

    @Test
    public void selectFields() {
        driver.insert 'users', json {
            name 'Name1'
            age 26
        }
        driver.insert 'users', json {
            name 'Name2'
            age 27
        }
        driver.insert 'users', json {
            name 'Name3'
            age 26
        }

        def res1 = driver.find 'users', json {
            age 27
        }, json {
            name 1
        }
        assertEquals(1, res1.length())
        assertEquals(['name'] as Set, res1.get(0).keySet());
        assertEquals('Name2', res1.get(0).name);

        def res2 = driver.find 'users', json {
            age 27
        }, json {
            name 1
            age 1
        }
        assertEquals(1, res2.length())
        assertEquals(['name', 'age'] as Set, res2.get(0).keySet());
        assertEquals('Name2', res2.get(0).name);
        assertEquals(27, res2.get(0).age);
    }

    private static def json(Closure closure) {
        def builder = new JsonBuilder()
        builder closure
        new JSONObject(builder as String)
    }
}
