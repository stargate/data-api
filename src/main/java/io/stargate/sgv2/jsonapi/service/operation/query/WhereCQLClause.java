package io.stargate.sgv2.jsonapi.service.operation.query;

import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.relation.OngoingWhereClause;
import io.stargate.sgv2.jsonapi.service.operation.tables.WhereCQLClauseAnalyzer;
import java.util.List;
import java.util.function.BiFunction;

/**
 * Interface for a class that can add the WHERE clause to a CQL query built using the Java Driver
 * Query Builder.
 *
 * <p>This is the WHERE part below:
 *
 * <pre>
 * UPDATE MyTable
 *   SET SomeColumn = 'SomeValue'
 *   WHERE columnName = B70DE1D0-9908-4AE3-BE34-5573E5B09F14;
 * </pre>
 *
 * The function should use the {@link OngoingWhereClause} to add the values to the statement,
 * typically using the {@link QueryBuilder#bindMarker()} method to add the value to the assignment
 * and adding the value second param as the positional value to bind.
 *
 * <p>NOTE: the {@link OngoingWhereClause} needs to know the type of the statement the where is
 * being added to (SELECT, DELETE, UPDATE), this is done through the static factory methods on the
 * {@link io.stargate.sgv2.jsonapi.service.operation.tables.TableWhereCQLClause} implementation.
 */
public interface WhereCQLClause<T extends OngoingWhereClause<T>>
    extends BiFunction<T, List<Object>, T>, CQLClause {

  WhereCQLClauseAnalyzer.WhereClauseAnalysis analyseWhereClause();
}
