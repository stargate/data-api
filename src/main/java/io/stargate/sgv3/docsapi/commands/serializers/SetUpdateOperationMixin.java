package io.stargate.sgv3.docsapi.commands.serializers;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonCreator.Mode;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import java.util.Map;

/**
 * Jackson Mixin for {@link SetUpdateOperation}
 *
 * <p>see {@link CommandSerializer} for details
 */
public abstract class SetUpdateOperationMixin {

  @JsonCreator(mode = Mode.DELEGATING)
  public SetUpdateOperationMixin(
      @JsonDeserialize(using = MapStringJsonDeserializer.class) Map<String, JsonNode> updates) {

    throw new UnsupportedOperationException();
  }
}
