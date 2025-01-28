package io.stargate.sgv2.jsonapi.api.model.command.clause.filter;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.databind.JsonNode;
import io.stargate.sgv2.jsonapi.api.model.command.JsonDefinition;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

/**
 * Intermediate lightly-processed container for JSON that specifies a {@link FilterClause}, and
 * allows for lazy deserialization into a {@link FilterClause}.
 */
@Schema(
    type = SchemaType.OBJECT,
    implementation = FilterClause.class,
    example =
        """
                     {"name": "Aaron", "country": "US"}
                      """)
public class FilterSpec extends JsonDefinition {
  /**
   * To deserialize the whole JSON value, need to ensure DELEGATING mode (instead of PROPERTIES).
   */
  @JsonCreator(mode = JsonCreator.Mode.DELEGATING)
  public FilterSpec(JsonNode json) {
    super(json);
  }
}
