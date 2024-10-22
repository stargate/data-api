package io.stargate.sgv2.jsonapi.service.resolver.sort;

import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import io.stargate.sgv2.jsonapi.api.model.command.Command;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.Sortable;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortExpression;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.exception.catchable.UnknownColumnException;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.filters.table.codecs.JSONCodecRegistry;
import io.stargate.sgv2.jsonapi.service.operation.query.OrderByCqlClause;
import io.stargate.sgv2.jsonapi.service.shredding.CqlNamedValue;
import org.jboss.resteasy.reactive.server.core.RuntimeExceptionMapper;

import java.util.Objects;

import static io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil.cqlIdentifierFromUserInput;

public class TableSortClauseResolver<CmdT extends Command & Sortable > extends SortClauseResolver<CmdT, TableSchemaObject> {

  private final JSONCodecRegistry codecRegistry;
  public TableSortClauseResolver(OperationsConfig operationsConfig, JSONCodecRegistry codecRegistry) {
    super(operationsConfig);
    this.codecRegistry = Objects.requireNonNull(codecRegistry, "codecRegistry must not be null");
  }

  @Override
  public OrderByCqlClause resolve(CommandContext<TableSchemaObject> commandContext, CmdT command) {

    var sortClause = command.sortClause();
    if (sortClause == null || sortClause.isEmpty()) {
      return OrderByCqlClause.NO_OP;
    }

    var vectorSorts = sortClause.tableVectorSorts();
    if (vectorSorts.size() > 1){
      throw new RuntimeException("TODO: throw too many ANN sorts");
    }

    if (vectorSorts.size() == 1){
      var vectorSort = vectorSorts.getFirst();


      return new TableANNOrderByCQlClause(commandContext.getSchemaObject(), vectorSort.column(), vectorSort.vector());
    }
  }

  private OrderByCqlClause resolveTableVectorSort(TableMetadata tableMetadata, SortExpression sortExpression) {

    var column = cqlIdentifierFromUserInput(sortExpression.path());
    CqlNamedValue cqlNamedValue = null;
    try {
      var codec = codecRegistry.codecToCQL(tableMetadata, column, sortExpression.vector());
      cqlNamedValue = new CqlNamedValue(tableMetadata, codec.toCQL(rawJsonValue));
    } catch (UnknownColumnException e) {
      // this should not happen, we checked above but the codecs are written to be very safe and
      // will check and throw
      throw ServerException.Code.UNEXPECTED_SERVER_ERROR.get(errVars(e));
    } catch (MissingJSONCodecException e) {
      unsupportedErrors.put(metadata, e);
    } catch (ToCQLCodecException e) {
      codecErrors.put(metadata, e);
    }
  }


}
