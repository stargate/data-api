package io.stargate.sgv2.jsonapi.service.operation.query;

import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.insert.OngoingValues;
import com.datastax.oss.driver.api.querybuilder.insert.RegularInsert;
import java.util.List;
import java.util.function.BiFunction;

/**
 * Interface for a class that can add the VALUES clause to a CQL query built using the Java Driver
 * Query Builder.
 *
 * <p>This is the values part below:
 *
 * <pre>
 * INSERT INTO users
 *    (key, age, country, human)
 * VALUES
 *    ('bob', 29, 'England', false);
 * </pre>
 *
 * The function should use the {@link OngoingValues} to add the values to the statement, typically
 * using the {@link QueryBuilder#bindMarker()} method to add the value to the assignment and adding
 * the value second param as the positional value to bind.
 */
public interface InsertValuesCQLClause
    extends BiFunction<OngoingValues, List<Object>, RegularInsert>, CQLClause {}
