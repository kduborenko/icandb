package org.kd.icandb

import groovy.json.JsonBuilder
import groovy.json.JsonSlurper
import org.junit.After
import org.junit.AfterClass
import org.junit.BeforeClass
import org.junit.Test
import org.kd.icandb.services.ICanDBService
import org.springframework.context.annotation.AnnotationConfigApplicationContext

import static org.junit.Assert.assertEquals

/**
 * @author Kiryl Dubarenka
 */
class ICanDBIntegrationTest {

    private static final def context = new AnnotationConfigApplicationContext("org.kd.icandb");
    private static ICanDB driver;

    @BeforeClass
    public static void setUp() {
        context.getBean(ICanDBService).start()
        driver = ICanDBDriver.getDriver("net://localhost:8978/")
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
        driver.insert 'users', [
            name: 'Name1',
            age: 26
        ]
    }

    @Test(expected = ICanDBException)
    public void insertUsingExistingID() {
        def id = driver.insert 'users', [
            name: 'Name1',
            age: 26
        ]
        driver.insert 'users', [
            _id: id,
            name: 'Name2',
            age: 27
        ]
    }

    @Test
    public void update() {
        def id = driver.insert 'users', [
            name: 'Name1',
            age: 26
        ]
        def updated = driver.update 'users', [
            _id: id
        ], [
            name: 'Name2',
            age: 27
        ]

        assertEquals(1, updated);

        def rs = driver.find('users').get(0) as Map
        assertEquals(id, rs._id);
        assertEquals('Name2', rs.name);
        assertEquals(27, rs.age);
    }

    @Test
    public void delete() {
        driver.insert 'users', [
            name: 'Name1',
            age: 26
        ]
        def id = driver.insert 'users', [
            name: 'Name2',
            age: 27
        ]
        def deleted = driver.delete 'users', [
            name: 'Name1'
        ]
        assertEquals(1, deleted);

        def rs = driver.find('users', [:]).get(0) as Map
        assertEquals(id, rs._id);
        assertEquals('Name2', rs.name);
        assertEquals(27, rs.age)
    }
    
    @Test
    public void findByField() {
        def id1 = driver.insert 'users', [
            name: 'Name1',
            age: 26
        ]
        driver.insert 'users', [
            name: 'Name2',
            age: 27
        ]
        def id3 = driver.insert 'users', [
            name: 'Name3',
            age: 26
        ]

        def res = driver.find 'users', [
            age: 26
        ]
        assertEquals([id1, id3] as Set, [res.get(0)._id, res.get(1)._id] as Set);
    }

    @Test
    public void selectFields() {
        driver.insert 'users', [
            name: 'Name1',
            age: 26
        ]
        driver.insert 'users', [
            name: 'Name2',
            age: 27
        ]
        driver.insert 'users', [
            name: 'Name3',
            age: 26
        ]

        def res1 = driver.find 'users', [
            age: 27
        ], [
            name: 1
        ]
        assertEquals([['name': 'Name2']], res1);

        def res2 = driver.find 'users', [
            age: 27
        ], [
            name: 1,
            age: 1
        ]
        assertEquals([['name': 'Name2', 'age': 27]], res2);
    }

    @Test
    public void compoundQuery() {
        driver.insert 'users', [
                name: 'Name1',
                age: 27
        ]
        def id = driver.insert 'users', [
                name: 'Name2',
                age: 27
        ]
        driver.insert 'users', [
                name: 'Name2',
                age: 26
        ]
        assertEquals([[_id: id]], driver.find('users', [
                name: 'Name2',
                age: 27
        ], [_id: 1]));
    }

    @Test
    public void inConditionQuery() {
        driver.insert 'users', [
                name: 'Name1',
                age: 27
        ]
        def id1 = driver.insert 'users', [
                name: 'Name2',
                age: 27
        ]
        def id2 = driver.insert 'users', [
                name: 'Name3',
                age: 26
        ]
        assertEquals([id1, id2] as Set, driver.find('users', [
                name: [$in: ['Name2', 'Name3']]
        ], [_id: 1])*._id as Set);
    }

    @Test
    public void orConditionQuery() {
        driver.insert 'users', [
                name: 'Name1',
                age: 27
        ]
        def id1 = driver.insert 'users', [
                name: 'Name2',
                age: 27
        ]
        def id2 = driver.insert 'users', [
                name: 'Name3',
                age: 26
        ]
        assertEquals([id1, id2] as Set, driver.find('users', [
                $or: [[name: 'Name2'], [name: 'Name3']]
        ], [_id: 1])*._id as Set);
    }

    @Test
    public void byIdIndex() {
        driver.insert 'users', [
                name: 'Name1',
                age: 27
        ]
        def id = driver.insert 'users', [
                name: 'Name2',
                age: 27
        ]
        driver.insert 'users', [
                name: 'Name3',
                age: 26
        ]
        println driver.explain('users', [_id: id])
        assertEquals(
                [
                        itemsInCollection: 3,
                        indexName        : "By ID index",
                        indexSize        : 3,
                        indexQuery       : [_id: id],
                        scanQuery        : [:],
                        scannedItems     : 1,
                        foundItems       : 1
                ],
                driver.explain('users', [_id: id])
        );
    }

    private static def json(Closure closure) {
        def builder = new JsonBuilder()
        builder closure
        builder as String
    }
}
