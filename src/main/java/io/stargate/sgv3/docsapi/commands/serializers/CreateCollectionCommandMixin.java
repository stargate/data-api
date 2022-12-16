package io.stargate.sgv3.docsapi.commands.serializers;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

/**
 * Jackson Mixin for {@link CreateCollectionCommandMixin}
 *
 * <p>see {@link CommandSerializer} for details
 */
@JsonTypeName("createCollection")
public class CreateCollectionCommandMixin {
  @JsonCreator
  public CreateCollectionCommandMixin(@JsonProperty("name") String name) {
    throw new UnsupportedOperationException();
  }
}
