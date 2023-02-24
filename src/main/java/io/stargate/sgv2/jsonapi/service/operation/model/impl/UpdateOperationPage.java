package io.stargate.sgv2.jsonapi.service.operation.model.impl;

import com.fasterxml.jackson.databind.JsonNode;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.CommandStatus;
import io.stargate.sgv2.jsonapi.service.shredding.model.DocumentId;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.function.Supplier;

public record UpdateOperationPage(
    int matchedCount,
    int modifiedCount,
    List<ReadAndUpdateOperation.UpdatedDocument> updatedDocuments,
    boolean returnDocs,
    boolean moreDataFlag)
    implements Supplier<CommandResult> {
  @Override
  public CommandResult get() {
    final DocumentId[] upsertedId = new DocumentId[1];
    List<JsonNode> updatedDocs = new ArrayList<>(updatedDocuments().size());
    updatedDocuments.forEach(
        update -> {
          if (update.upserted()) upsertedId[0] = update.id();
          if (returnDocs) updatedDocs.add(update.document());
        });
    EnumMap<CommandStatus, Object> updateStatus = new EnumMap<>(CommandStatus.class);
    if (upsertedId[0] != null) updateStatus.put(CommandStatus.UPSERTED_ID, upsertedId[0]);
    updateStatus.put(CommandStatus.MATCHED_COUNT, matchedCount());
    updateStatus.put(CommandStatus.MODIFIED_COUNT, modifiedCount());
    if (moreDataFlag) updateStatus.put(CommandStatus.MORE_DATA, moreDataFlag);
    if (returnDocs) {
      return new CommandResult(new CommandResult.ResponseData(updatedDocs), updateStatus);
    } else {
      return new CommandResult(updateStatus);
    }
  }
}
