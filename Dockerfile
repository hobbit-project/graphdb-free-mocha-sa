FROM java

ADD repo-config.ttl /graphdb/config/

ADD target/graphdb-1.0.0.jar /graphdb/graphdb.jar

WORKDIR /graphdb

CMD java -cp /graphdb/graphdb.jar org.hobbit.core.run.ComponentStarter eu.hobbit.mocha.systems.graphdb.GraphDBSystemAdapter

