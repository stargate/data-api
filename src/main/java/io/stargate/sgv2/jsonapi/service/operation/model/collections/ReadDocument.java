package io.stargate.sgv2.jsonapi.service.operation.model.collections;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Preconditions;
import io.stargate.sgv2.jsonapi.service.operation.model.DocumentSource;
import io.stargate.sgv2.jsonapi.service.shredding.model.DocumentId;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Supplier;

/**
 * Represents a document read from the database
 *
 * @param id Document Id identifying the document
 * @param txnId Unique UUID resenting point in time of a document, used for LWT transactions. This
 *     will be missing when the Document was created for an upsert, and there is code in the {@link
 *     ReadAndUpdateOperation} that uses this as the market. Optuonal is used to allow for the case
 *     where the document is from upsert
 * @param docSupplier JsonNode representation of the document
 * @param sortColumns List<JsonNode> Serialized sort column value
 */
public record ReadDocument(
    Optional<DocumentId> id,
    java.util.Optional<UUID> txnId,
    Supplier<JsonNode> docSupplier,
    List<JsonNode> sortColumns)
    implements DocumentSource {

  /// TODO AARON - comments

  public ReadDocument {
    Preconditions.checkNotNull(txnId, "txnId cannot be null");
    Preconditions.checkNotNull(docSupplier, "docSupplier cannot be null");
    Preconditions.checkNotNull(sortColumns, "sortColumns cannot be null");
  }

  public static ReadDocument from(DocumentId id, UUID txnId, JsonNode document) {
    return new ReadDocument(
        Optional.ofNullable(id), Optional.ofNullable(txnId), () -> document, List.of());
  }

  public static ReadDocument from(
      DocumentId id, UUID txnId, Supplier<JsonNode> docSupplier, List<JsonNode> sortColumns) {
    return new ReadDocument(
        Optional.ofNullable(id), Optional.ofNullable(txnId), docSupplier, sortColumns);
  }

  //  public ReadDocument replaceDocSupplier(Supplier<JsonNode> docSupplier) {
  //    // TODO: the old code would let this happen
  //    return new ReadDocument(id, txnId, docSupplier, sortColumns);
  //  }
  //
  //  public ReadDocument replaceDocSupplier(JsonNode doc) {
  //    return replaceDocSupplier(() -> doc);
  //  }

  @Override
  public JsonNode get() {
    return docSupplier.get();
  }
}
