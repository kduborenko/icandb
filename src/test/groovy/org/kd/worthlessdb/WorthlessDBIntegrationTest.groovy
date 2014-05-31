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
    public void echo() {
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
    public void insert() {
        Socket socket = new Socket("localhost", 8978);
        BufferedReader br = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        PrintWriter pw = new PrintWriter(socket.getOutputStream(), true);
        insertObject pw, br, {
            $collection 'users'
            $obj {
                name 'Name1'
                age 26
            }
        }
    }
    
    @Test
    public void findByField() {
        Socket socket = new Socket("localhost", 8978);
        BufferedReader br = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        PrintWriter pw = new PrintWriter(socket.getOutputStream(), true);
        def id1 = UUID.randomUUID().toString()
        def id2 = UUID.randomUUID().toString()
        def id3 = UUID.randomUUID().toString()
        insertObject pw, br, {
            $collection 'users2'
            $obj {
                _id id1
                name 'Name1'
                age 26
            }
        }
        insertObject pw, br, {
            $collection 'users2'
            $obj {
                _id id2
                name 'Name2'
                age 27
            }
        }
        insertObject pw, br, {
            $collection 'users2'
            $obj {
                _id id3
                name 'Name3'
                age 26
            }
        }

        pw.println(json {
            $op 'find'
            $arg {
                $collection 'users2'
                $query {
                    age 26
                }
            }
        })
        assertEquals([
                $status: 'ok',
                $res: unorderedCollection([
                        [name: 'Name1', age: 26, _id: id1],
                        [name: 'Name3', age: 26, _id: id3]
                ])
        ], parse(br.readLine()));

        pw.println(json {
            $op 'find'
            $arg {
                $collection 'users2'
                $query {
                    age 26
                }
                $fields {
                    name 1
                }
            }
        })
        assertEquals([
                $status: 'ok',
                $res: unorderedCollection([
                        [name: 'Name1'],
                        [name: 'Name3']
                ])
        ], parse(br.readLine()));
    }

    private static void insertObject(PrintWriter pw, BufferedReader br, obj) {
        pw.println(json {
            $op 'insert'
            $arg obj
        });
        assertEquals([$status: 'ok', $res: [$inserted: 1, $id: $anyObject]], parse(br.readLine()));
    }

    private static def $anyObject = new Object() {
        @Override
        boolean equals(Object obj) {
            return true
        }
    }

    private static def unorderedCollection(def collection) {
        new HashSet(collection) {
            @SuppressWarnings("GroovyAssignabilityCheck")
            @Override
            boolean equals(def o) {
                return super.equals(new HashSet(o))
            }
        }
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
