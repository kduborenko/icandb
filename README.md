_"We Made You Because We Could"_ - Charlie Holloway. Prometheus, 2012

## Overview

ICanDB is a NoSQL document-oriented storage written in Java.

_ICanDB doesn't set a goal to be better than other databases. It isn't going to be most efficient DB in the world or have ideal code and documentation. It is a project where I can play with some technologies or learn some algorithms by implementing and applying it in "real" project._

## Instructions

To compile and start working with database you need to have installed JDK 8.

### Build and verify

    $ ./gradlew build
    
### Run client shell with embedded in-memory db

    $ ./gradlew client-shell:shell
    
### Run network database service

    $ ./gradlew app:execute
    
### Run client shell and connect to remote database

    $ ./gradlew client-shell:shell -Dconnect=net://<host>:8978/
    
## Client shell

Client shell is just a regular groovy shell with predefined set of objects and functions.

    groovy:000> db.insert("w", [name:'N1', age:26])
    ===> 7003a1b3-4633-40e2-b9fe-2ca00a0b0f05
    groovy:000> db.insert("w", [name:'N2', age:27])
    ===> d90ffa96-d578-40fe-be56-8b785778d57b
    groovy:000> db.insert("w", [name:'N3', age:27])
    ===> 99c968b8-238b-42ea-acc2-851afedf08a4
    groovy:000> db.insert("w", [name:'N4', age:28])
    ===> 88cb6add-7635-447a-ae20-f517ed6b8112
    groovy:000> db.find("w",[$or:[[name:'N2'],[age:28]]])
    ===> [[name:N2, age:27, _id:d90ffa96-d578-40fe-be56-8b785778d57b], [name:N4, age:28, _id:88cb6add-7635-447a-ae20-f517ed6b8112]]

## Roadmap

I defined set of global goals to achieve and features to implement, but over time I can change it:

1. **Phase #1**:
    * Implement basic components of database:
         * Networking sub-system
         * In-memory storage
         * Application launcher
    * Implement in-memory embedded and network drivers
    * Create set of basic operations with limited query language
2. **Phase #2**:
    * Indexes
    * Extend query language
3. **Phase #3**:
    * Persistent storage
4. **Phase #4**:
    * Transactions
    * Rich query language (joins, aggregations, etc.)

    _Yes, in NoSQL database_

**Future**: sharding, replication, geohash, etc.

## Technologies

* _Java 8_ - because I like Java, and just released Java 8 has lambdas, method references, and higher-order functions (features I waited so long, sometimes I will use them even where it is unnecessary, because I want to play with it)
* _Gradle_ - this building tool is gaining popularity in Java projects, but I still not tried it yet
* _Springframework_ - latest version I used was 3.0, now it is 4.0. Don't want to fall behind
