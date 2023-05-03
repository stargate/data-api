package io.stargate.sgv2.jsonapi.api.model.command;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import io.quarkus.runtime.annotations.RegisterForReflection;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;

/**
 * Interface that Commands that only take "empty" Command (that is, do not expose options but should
 * allow empty JSON Object nonetheless) should implement. <br>
 * NOTE: if {@code Command} starts accepting options, it should NO LONGER implement this interface
 * as combination will probably not work.
 */
@RegisterForReflection
public interface NoOptionsCommand {
  @JsonProperty("options")
  default void setOptions(JsonNode value) throws JsonApiException {
    // Empty JSON Object and null are accepted; anything else failure
    if (value.isNull() || (value.isObject() && value.isEmpty())) {
      return;
    }
    final String msg =
        String.format(
            "%s: %s",
            ErrorCode.COMMAND_ACCEPTS_NO_OPTIONS.getMessage(), getClass().getSimpleName());
    throw new JsonApiException(ErrorCode.COMMAND_ACCEPTS_NO_OPTIONS, msg);
  }
}
