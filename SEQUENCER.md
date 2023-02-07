# Sequencer Usage

The implementation of the engine operations is done by using the query sequence framework.
This internal framework was defined in order to abstract the execution of the queries from the operations.
Operations define what queries need to be done and don't know anything about the execution.

Execution part is offloaded to the executors.
Currently, there is only one implementation of the executor, [ReactiveQueryExecutor](src/main/java/io/stargate/sgv3/docsapi/service/bridge/executor/ReactiveQueryExecutor.java), but others might be added in the future.
Adding new executors, does not require any changes in the implemented operations.

## Construction

Here are some examples that help you get started with the sequencer framework.

### Single query

```java
import java.util.*;
import io.stargate.*;

class SingleQuery {

    public QuerySequenceSink<Supplier<CommandResult>> sequence() {
        QueryOuterClass.Query query;
        
        return QuerySequence.query(query, QueryOptions.Type.READ)
                .sink(resultSet -> {
                    // implement result handing
                });
    }

}
```

### Multiple query

```java
import java.util.*;
import io.stargate.*;

class MultipleQueries {

    public QuerySequenceSink<Supplier<CommandResult>> sequence() {
        QueryOuterClass.Query query1;
        QueryOuterClass.Query query2;
        List<QueryOuterClass.Query> queries = List.of(query1, query2);
        
        return QuerySequence.queries(queries, QueryOptions.Type.READ)
                .sink(resultSetList -> {
                    // implement result handing
                });
    }

}
```

### Query properties

#### Define query type
When constructing a sequence part, you need to define type of the query that is executed.
This way, the C* consistency will be directly populated with the default settings, if it was not explicitly defined in the query itself.

```java
import java.util.*;
import io.stargate.*;

class SingleWriteQuery {

    public QuerySequenceSink<Supplier<CommandResult>> sequence()  {
        QueryOuterClass.Query query;
        
        return QuerySequence.query(query, QueryOptions.Type.WRITE)
                .sink(resultSet -> {
                    // implement result handing
                });
    }

}
```

#### Define query options
In case of the `READ` queries you can define page size and paging state.

> NOTE: When running multiple queries, defined options apply to all queries.

```java
import java.util.*;
import io.stargate.*;

class SingleQueryWithOptions {

    public QuerySequenceSink<Supplier<CommandResult>> sequence()  {
        QueryOuterClass.Query query;
        
        return QuerySequence.query(query, QueryOptions.Type.READ)
                .withPageSize(10)
                .withPagingState("some-state")
                .sink(resultSet -> {
                    // implement result handing
                });
    }

}
```

### Custom handler
By default, the output of the sequence part is the `ResultSet` received from the query execution.
Exceptions are not handled, and are directly thrown.
In case you want to handle exception yourself, you can to that using the custom handler.
Handler can change the output of the sequence part to anything you want.

```java
import java.util.*;
import io.stargate.*;

class SingleQueryWithHandler {

    public QuerySequenceSink<Supplier<CommandResult>> sequence()  {
        QueryOuterClass.Query query;
        
        return QuerySequence.query(query, QueryOptions.Type.READ)
                .withHandler((resultSet, throwable) -> {
                    return throwable != null;
                })
                .sink(error -> {
                    if (error) {
                        // implement result handing in error case
                    } else {
                        // implement result with no error
                    }
                });
    }

}
```

In case of the multiple queries, the handler method signature includes the index of the given query.

```java
import java.util.*;
import io.stargate.*;

class MultipleQueriesWithHandler {

    public QuerySequenceSink<Supplier<CommandResult>> sequence()  {
        QueryOuterClass.Query query1;
        QueryOuterClass.Query query2;
        List<QueryOuterClass.Query> queries = List.of(query1, query2);
        
        return QuerySequence.queries(queries, QueryOptions.Type.READ)
                .withHandler((resultSet, throwable, index) -> {
                    return throwable != null;
                })
                .sink(errorList -> {
                    if (errorList.contains(Boolean.TRUE)) {
                        // implement result handing in error case
                    } else {
                        // implement result with no error
                    }
                });
    }

}
```

### Piping
Piping is used when you want to construct a multi-step query sequence.
In this case, result of the previous step is available in order to construct next queries (if needed).
Each step can define different query type and options.

All types of pipes are available:

1. Single query pipe to single query
2. Single query pipe to multiple queries
3. Multiple queries pipe to single query
4. Multiple queries pipe to multiple queries

Here is an example:

```java
import java.util.*;

import io.stargate.sgv2.jsonapi.service.sequencer.QueryOptions;

class SingleQueryToMultipleQueriesPipe {

    public QuerySequenceSink<Supplier<CommandResult>> sequence() {
        QueryOuterClass.Query query1;

        return QuerySequence.query(query1, QueryOptions.Type.READ)

                // pipe
                .then()
                .queries(resultSet -> {
                    // pipe from result of query1
                    QueryOuterClass.Query query2;
                    QueryOuterClass.Query query3;
                    return List.of(query2, query3);
                }, QueryOptions.Type.WRITE)

                // sink
                .sink(resultSetList -> {
                    // result handling of query2 & query3
                });
    }

}
```

#### Pipe to sink
In some cases you want to have two-way pipe after a sequence step.
This allows for fine handling of the query results and decision-making.

```java
import java.util.*;

import io.stargate.sgv2.jsonapi.service.sequencer.QueryOptions;
import io.stargate.sgv2.jsonapi.service.sequencer.QuerySequence;

class PipeToSink {

    public QuerySequenceSink<Supplier<CommandResult>> sequence() {
        QueryOuterClass.Query query1;
        QueryOuterClass.Query query2;
        List<QueryOuterClass.Query> firstQueries = List.of(query1, query2);

        return QuerySequence.queries(firstQueries, QueryOptions.Type.READ)

                // simple handler
                .withHandler((resultSet, throwable, index) -> {
                    return throwable;
                })

                // pipe to sink
                .then()
                .pipeToSink(errors -> {
                    // in case of errors return error result
                    // otherwise process with queries
                    if (!errors.isEmpty()) {
                        return QuerySequence.empty().sink(v -> {
                            // implement error result
                        });
                    } else {
                        // pipe to next step
                        QueryOuterClass.Query query3;
                        QueryOuterClass.Query query4;
                        List<QueryOuterClass.Query> queries = List.of(query3, query4);
                        return QuerySequence.queries(queries, QueryOptions.Type.READ)
                                .sink(resultSetList -> {
                                    // implement result handing of query3 & query4
                                });
                    }
                });
    }

}
```

## Running

```java
import java.util.*;
import io.stargate.*;

class RunReactive {

    public Uni<Supplier<CommandResult>> run() {
        QueryOuterClass.Query query;
        QuerySequenceSink<Supplier<CommandResult>> sequence = QuerySequence.query(query, QueryOptions.Type.READ)
                .sink(resultSet -> {
                    // implement result handing
                });
        return sequence.reactive().execute(executor);
    }

}
```