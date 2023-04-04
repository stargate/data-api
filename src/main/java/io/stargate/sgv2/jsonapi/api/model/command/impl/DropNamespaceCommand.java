package io.stargate.sgv2.jsonapi.api.model.command.impl;

import com.fasterxml.jackson.annotation.JsonTypeName;
import io.stargate.sgv2.jsonapi.api.model.command.GeneralCommand;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;
import javax.validation.constraints.Size;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

@Schema(description = "Command that deletes a namespace.")
@JsonTypeName("dropNamespace")
public record DropNamespaceCommand(
    @NotNull
        @Pattern(regexp = "[a-zA-Z][a-zA-Z0-9_]*")
        @Pattern(regexp = "^(?!system$).*")
        @Size(min = 1, max = 48)
        @Schema(description = "Name of the namespace")
        String name)
    implements GeneralCommand {
  // TODO add NoOptions extension
}
