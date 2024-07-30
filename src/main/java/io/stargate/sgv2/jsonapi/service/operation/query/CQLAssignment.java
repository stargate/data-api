package io.stargate.sgv2.jsonapi.service.operation.query;

import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.update.OngoingAssignment;
import com.datastax.oss.driver.api.querybuilder.update.UpdateWithAssignments;
import java.util.List;
import java.util.function.BiFunction;

/**
 * Interface for any class that can be used to assign values to a CQL Update statement build with the
 * Java Query Builder.
 * <p>
 * For example building the SET in the following CQL:
 * <pre>
 *   UPDATE MyTable
 *   SET SomeColumn = 'SomeValue'
 *   WHERE columnName = B70DE1D0-9908-4AE3-BE34-5573E5B09F14;
 * </pre>
 * <p>
 * Always used with the {@link UpdateValuesCQLClause} to build the full clause.
 * <p>
 * The function should use the {@link OngoingAssignment} to add the values to the assignment, typically using the
 * {@link QueryBuilder#bindMarker()} method to add the value to the assignment and adding the value second param as the
 * positional value to bind.
 */
@FunctionalInterface
public interface CQLAssignment
    extends BiFunction<OngoingAssignment, List<Object>, UpdateWithAssignments> {}
