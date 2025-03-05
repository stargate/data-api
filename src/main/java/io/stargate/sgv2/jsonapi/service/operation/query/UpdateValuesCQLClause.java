package io.stargate.sgv2.jsonapi.service.operation.query;

import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.update.OngoingAssignment;
import com.datastax.oss.driver.api.querybuilder.update.UpdateWithAssignments;
import io.stargate.sgv2.jsonapi.service.shredding.Deferrable;

import java.util.List;
import java.util.function.BiFunction;

/**
 * Interface for a class that can add the SET clause to a CQL query built using the Java Driver
 * Query Builder.
 *
 * <p>This is the set part below:
 *
 * <pre>
 * UPDATE MyTable
 *   SET SomeColumn = 'SomeValue'
 *   WHERE columnName = B70DE1D0-9908-4AE3-BE34-5573E5B09F14;
 * </pre>
 *
 * The function should use the {@link OngoingAssignment} to add the values to the statement,
 * typically using the {@link QueryBuilder#bindMarker()} method to add the value to the assignment
 * and adding the value second param as the positional value to bind.
 *
 * <p>Supports {@link Deferrable} so that the vectorize values can be deferred
 * until execution time. See {@link #deferredValues()} for docs.
 */
public interface UpdateValuesCQLClause
    extends BiFunction<OngoingAssignment, List<Object>, UpdateWithAssignments>, CQLClause, Deferrable {}
