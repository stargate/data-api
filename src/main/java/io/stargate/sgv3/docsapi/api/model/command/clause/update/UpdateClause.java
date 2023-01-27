package io.stargate.sgv3.docsapi.api.model.command.clause.update;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.stargate.sgv3.docsapi.api.model.command.deserializers.UpdateClauseDeserializer;
import java.util.EnumMap;
import java.util.List;
import java.util.stream.Collectors;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

/**
 * Value type read from incoming JSON and used for resolving into actual {@link UpdateOperation}s
 * with additional context.
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
  public List<UpdateOperation> getUpdateOperations() {
    return updateOperationDefs.entrySet().stream()
        .map(e -> e.getKey().resolveOperation(e.getValue()))
        .collect(Collectors.toList());
  }
}
