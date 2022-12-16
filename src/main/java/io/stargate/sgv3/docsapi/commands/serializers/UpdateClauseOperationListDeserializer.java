package io.stargate.sgv3.docsapi.commands.serializers;

import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import io.stargate.sgv3.docsapi.commands.clauses.update.SetUpdateOperation;
import io.stargate.sgv3.docsapi.commands.clauses.update.UpdateClauseOperation;
import java.io.IOException;

/**
 * Creates the {@link UpdateClauseOperation}s from the query.
 *
 * <p>NOTE: Every subclass of {@link UpdateClauseOperation} needs to be registered here
 *
 * <p>This is where we map the $set to {@link SetUpdateOperation} etc, e.g.
 *
 * <pre>
 * {"$set" : {"updated_field" : "new value"}, "$unset" : {"old field" : ""}}
 * </pre>
 *
 * Extensive notes in {@link MapToListDeserializer}
 */
public class UpdateClauseOperationListDeserializer
    extends MapToListDeserializer<UpdateClauseOperation> {

  @Override
  protected UpdateClauseOperation create(DeserializationContext ctxt, String key, JsonNode value)
      throws IOException {
    switch (key) {
      case "$set":
        return ctxt.readTreeAsValue(value, SetUpdateOperation.class);
      default:
        return null;
    }
  }
}
