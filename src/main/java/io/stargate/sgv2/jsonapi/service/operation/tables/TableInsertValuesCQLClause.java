package io.stargate.sgv2.jsonapi.service.operation.tables;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.bindMarker;

import com.datastax.oss.driver.api.querybuilder.insert.OngoingValues;
import com.datastax.oss.driver.api.querybuilder.insert.RegularInsert;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.query.InsertValuesCQLClause;
import io.stargate.sgv2.jsonapi.service.resolver.UnvalidatedClauseException;
import io.stargate.sgv2.jsonapi.service.shredding.tables.WriteableTableRow;
import io.stargate.sgv2.jsonapi.util.PrettyPrintable;
import java.util.List;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link InsertValuesCQLClause} that builds the VALUES part of the CQL Insert statement when
 * using the Java Driver Query Builder.
 *
 * <p>
 *
 * @param tableSchemaObject The target {@link TableSchemaObject} we are inserting into.
 * @param row The {@link WriteableTableRow} shredded document for inserting the document into a
 *     table
 */
public record TableInsertValuesCQLClause(TableSchemaObject tableSchemaObject, WriteableTableRow row)
    implements InsertValuesCQLClause {

  private static final Logger LOGGER = LoggerFactory.getLogger(TableInsertValuesCQLClause.class);

  public TableInsertValuesCQLClause {
    Objects.requireNonNull(tableSchemaObject, "tableSchemaObject cannot be null");
    Objects.requireNonNull(row, "row cannot be null");
    if (row.allColumns().isEmpty()) {
      throw new UnvalidatedClauseException("Row must have at least one column to insert");
    }
  }

  @Override
  public RegularInsert apply(OngoingValues ongoingValues, List<Object> positionalValues) {
    Objects.requireNonNull(ongoingValues, "ongoingValues cannot be null");
    Objects.requireNonNull(positionalValues, "positionalValues cannot be null");

    RegularInsert regularInsert = null;

    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(
          "apply() - building insert keyspace={} table={} row.allColumns={}",
          tableSchemaObject.keyspaceName().asCql(true),
          tableSchemaObject.keyspaceName().asCql(true),
          PrettyPrintable.pprint(row.allColumns()));
    }
    for (var cqlNamedValue : row.allColumns().values()) {
      positionalValues.add(cqlNamedValue.value());
      regularInsert =
          regularInsert == null
              ? ongoingValues.value(cqlNamedValue.name(), bindMarker())
              : regularInsert.value(cqlNamedValue.name(), bindMarker());
    }

    return regularInsert;
  }
}
