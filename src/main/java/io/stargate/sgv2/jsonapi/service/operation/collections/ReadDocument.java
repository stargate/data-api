package io.stargate.sgv2.jsonapi.service.operation.collections;

import com.fasterxml.jackson.databind.JsonNode;
import io.stargate.sgv2.jsonapi.service.operation.DocumentSource;
import io.stargate.sgv2.jsonapi.service.shredding.collections.DocumentId;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Supplier;

/**
 * Represents a document read from the database
 *
 * @param id Document Id identifying the document
 * @param txnId Unique UUID resenting point in time of a document, used for LWT transactions. This
 *     will be missing when the Document was created for an upsert, and there is code in the {@link
 *     ReadAndUpdateCollectionOperation} that uses this as the market. Optuonal is used to allow for
 *     the case where the document is from upsert
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
    Objects.requireNonNull(txnId, "txnId cannot be null");
    Objects.requireNonNull(docSupplier, "docSupplier cannot be null");
    Objects.requireNonNull(sortColumns, "sortColumns cannot be null");
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
