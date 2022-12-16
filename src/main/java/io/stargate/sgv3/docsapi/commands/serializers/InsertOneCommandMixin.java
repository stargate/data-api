package io.stargate.sgv3.docsapi.commands.serializers;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.JsonNode;

/**
 * Jackson Mixin for {@link InsertOneCommand}
 *
 * <p>see {@link CommandSerializer} for details
 */
@JsonTypeName("insertOne")
public abstract class InsertOneCommandMixin {

  @JsonCreator
  public InsertOneCommandMixin(@JsonProperty("document") JsonNode document) {

    throw new UnsupportedOperationException();
  }
}
