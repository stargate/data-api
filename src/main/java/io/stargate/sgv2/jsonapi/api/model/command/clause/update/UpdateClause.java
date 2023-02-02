package io.stargate.sgv2.jsonapi.api.model.command.clause.update;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.stargate.sgv2.jsonapi.api.model.command.deserializers.UpdateClauseDeserializer;
import io.stargate.sgv2.jsonapi.exception.DocsException;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Set;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

/**
 * Value type read from incoming JSON and used for resolving into actual {@link UpdateOperation}s
 * with validation and additional context.
 */
@JsonDeserialize(using = UpdateClauseDeserializer.class)
@Schema(
    type = SchemaType.OBJECT,
    implementation = Object.class,
    example =
        """
             {"$set" : {"location": "New York"},
              "$unset" : {"new_data": 1}
              """)
public record UpdateClause(EnumMap<UpdateOperator, ObjectNode> updateOperationDefs) {
  /**
   * Method that will validate update operation definitions of the clause and construct an ordered
   * set of {@link UpdateOperation}s.
   *
   * @return List of update operations to execute
   */
  public List<UpdateOperation> buildOperations() {
    // First, resolve operations individually; this will handle basic validation
    var operationMap = new EnumMap<UpdateOperator, UpdateOperation>(UpdateOperator.class);
    updateOperationDefs
        .entrySet()
        .forEach(e -> operationMap.put(e.getKey(), e.getKey().resolveOperation(e.getValue())));

    // Then handle cross-operation validation

    // First: verify $set and $unset do NOT have overlapping keys

    SetOperation setOp = (SetOperation) operationMap.get(UpdateOperator.SET);
    UnsetOperation unsetOp = (UnsetOperation) operationMap.get(UpdateOperator.UNSET);

    if ((setOp != null) && (unsetOp != null)) {
      Set<String> paths = setOp.paths();
      paths.retainAll(unsetOp.paths());

      if (!paths.isEmpty()) {
        throw new DocsException(
            ErrorCode.UNSUPPORTED_UPDATE_OPERATION_PARAM,
            "Update operators '$set' and '$unset' must not refer to same path: '%s'"
                .formatted(paths.iterator().next()));
      }
    }

    return new ArrayList<>(operationMap.values());
  }
}
