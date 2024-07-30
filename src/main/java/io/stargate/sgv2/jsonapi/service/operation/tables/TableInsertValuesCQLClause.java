package io.stargate.sgv2.jsonapi.service.operation.tables;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.bindMarker;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.querybuilder.insert.OngoingValues;
import com.datastax.oss.driver.api.querybuilder.insert.RegularInsert;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.filters.table.codecs.JSONCodecRegistry;
import io.stargate.sgv2.jsonapi.service.operation.filters.table.codecs.MissingJSONCodecException;
import io.stargate.sgv2.jsonapi.service.operation.filters.table.codecs.ToCQLCodecException;
import io.stargate.sgv2.jsonapi.service.operation.filters.table.codecs.UnknownColumnException;
import io.stargate.sgv2.jsonapi.service.operation.query.InsertValuesCQLClause;
import io.stargate.sgv2.jsonapi.service.resolver.UnvalidatedClauseException;
import io.stargate.sgv2.jsonapi.service.shredding.tables.WriteableTableRow;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * A {@link InsertValuesCQLClause} that builds the VALUES part of the CQL Insert statement when using the
 * Java Driver Query Builder.
 * <p>
 * @param tableSchemaObject The target {@link TableSchemaObject} we are inserting into.
 * @param row The {@link WriteableTableRow} shredded document for inserting the document into a table
 */
public record TableInsertValuesCQLClause(TableSchemaObject tableSchemaObject, WriteableTableRow row)
    implements InsertValuesCQLClause {

  public TableInsertValuesCQLClause {
    Objects.requireNonNull(tableSchemaObject, "tableSchemaObject cannot be null");
    Objects.requireNonNull(row, "row cannot be null");
    if (row.allColumnValues().isEmpty()) {
      throw new UnvalidatedClauseException("Row must have at least one column to insert");
    }
  }

  @Override
  public RegularInsert apply(OngoingValues ongoingValues, List<Object> positionalValues) {
    Objects.requireNonNull(ongoingValues, "ongoingValues cannot be null");
    Objects.requireNonNull(positionalValues, "positionalValues cannot be null");

    RegularInsert regularInsert = null;

    for (Map.Entry<CqlIdentifier, Object> entry : row.allColumnValues().entrySet()) {
      try {
        var codec =
            JSONCodecRegistry.codecToCQL(
                tableSchemaObject.tableMetadata, entry.getKey(), entry.getValue());
        positionalValues.add(codec.toCQL(entry.getValue()));
      } catch (UnknownColumnException e) {
        // TODO AARON - Handle error
        throw new RuntimeException(e);
      } catch (MissingJSONCodecException e) {
        // TODO AARON - Handle error
        throw new RuntimeException(e);
      } catch (ToCQLCodecException e) {
        // TODO AARON - Handle error
        throw new RuntimeException(e);
      }
      regularInsert =
          regularInsert == null
              ? ongoingValues.value(entry.getKey(), bindMarker())
              : regularInsert.value(entry.getKey(), bindMarker());
    }

    return regularInsert;
  }
}
