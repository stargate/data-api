package io.stargate.sgv2.jsonapi.service.resolver.update;

import static io.stargate.sgv2.jsonapi.exception.ErrorFormatters.errFmt;
import static io.stargate.sgv2.jsonapi.exception.ErrorFormatters.errVars;

import com.fasterxml.jackson.databind.node.ObjectNode;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.JsonLiteral;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.JsonType;
import io.stargate.sgv2.jsonapi.api.model.command.clause.update.UpdateOperator;
import io.stargate.sgv2.jsonapi.exception.UpdateException;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.query.ColumnAssignment;
import io.stargate.sgv2.jsonapi.service.operation.query.ColumnSetToAssignment;
import io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil;
import java.util.List;

/** Resolver to resolve $unset argument to List of ColumnAssignment. */
public class TableUpdateUnsetResolver implements TableUpdateOperatorResolver {

  /**
   * Resolve the {@link UpdateOperator#UNSET} operation.
   *
   * <p>Example: (Note, it does not matter what the unset value is, NULL will be set)
   *
   * <ul>
   *   <li>primitive column<code>{"$unset" : { "age" : 1 , "human" : false}}</code>
   *   <li>list column<code>{"$unset" : { "listColumn" : "abc"}}</code>
   *   <li>set column<code>{"$unset" : { "setColumn" : {"random":"random}}}</code>
   *   <li>map column<code>{"$unset" : { "mapColumn" : []}</code>
   * </ul>
   *
   * @param table tableSchemaObject
   * @param arguments arguments objectNode for the $unset
   * @return list of columnAssignments for all the $unset column updates
   */
  @Override
  public List<ColumnAssignment> resolve(TableSchemaObject table, ObjectNode arguments) {
    // Checking if the columns exist in the table should be validated in the clause validation
    // we ignore the value, API spec says it should be empty string and we should validate that in
    // the API tier
    return arguments.properties().stream()
        .map(
            entry -> {
              var column = entry.getKey();
              var apiColumnDef = checkUpdateColumnExists(table, column);
              if (!apiColumnDef.type().apiSupport().update().set()) {
                throw UpdateException.Code.UNSUPPORTED_UPDATE_OPERATOR.get(
                    errVars(
                        table,
                        map -> {
                          map.put("operator", "$set");
                          map.put("targetColumn", errFmt(apiColumnDef.name()));
                        }));
              }

              return new ColumnAssignment(
                  new ColumnSetToAssignment(),
                  table.tableMetadata(),
                  CqlIdentifierUtil.cqlIdentifierFromUserInput(entry.getKey()),
                  new JsonLiteral<>(null, JsonType.NULL));
            })
        .toList();
  }
}
