package io.stargate.sgv2.jsonapi.api.model.command.impl;

import com.fasterxml.jackson.annotation.JsonTypeName;
import io.stargate.sgv2.jsonapi.api.model.command.CommandName;
import io.stargate.sgv2.jsonapi.api.model.command.NoOptionsCommand;
import io.stargate.sgv2.jsonapi.api.model.command.ReadCommand;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

@Schema(
    description =
        "Command that returns estimated count of documents in a collection based on the collection.")
@JsonTypeName(CommandName.Names.ESTIMATED_DOCUMENT_COUNT)
public record EstimatedDocumentCountCommand() implements ReadCommand, NoOptionsCommand {

  /** {@inheritDoc} */
  @Override
  public CommandName commandName() {
    return CommandName.ESTIMATED_DOCUMENT_COUNT;
  }
}
