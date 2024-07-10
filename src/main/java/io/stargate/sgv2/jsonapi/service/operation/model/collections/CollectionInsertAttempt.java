package io.stargate.sgv2.jsonapi.service.operation.model.collections;

import io.stargate.sgv2.jsonapi.service.operation.model.InsertAttempt;
import io.stargate.sgv2.jsonapi.service.shredding.DocRowIdentifer;
import io.stargate.sgv2.jsonapi.service.shredding.WritableDocRow;
import io.stargate.sgv2.jsonapi.service.shredding.model.DocumentId;
import io.stargate.sgv2.jsonapi.service.shredding.model.WritableShreddedDocument;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Container for an individual Document insertion attempt: used to keep track of the original input
 * position; document (if available), its id (if available) and possible processing error.
 * Information will be needed to build optional detail response (returnDocumentResponses).
 */
public class CollectionInsertAttempt implements InsertAttempt {

  // TODO: AARON moved out of the inner class to make sure the encapsulation was correct, previously
  // InsertOperation
  // was using private fields from this class

  private final int position;
  public final WritableShreddedDocument document;
  private final DocumentId documentId;

  private Throwable failure;

  public CollectionInsertAttempt(int position, DocumentId documentId, Throwable failure) {
    this.position = position;
    // TODO confirm why the document is allowed to be null
    this.document = null;
    this.documentId = documentId;
    this.failure = failure;
  }

  private CollectionInsertAttempt(int position, WritableShreddedDocument document) {
    this.position = position;
    this.document = document;
    this.documentId = document.id();
  }

  public static CollectionInsertAttempt from(int position, WritableShreddedDocument document) {
    return new CollectionInsertAttempt(position, document);
  }

  public static List<CollectionInsertAttempt> from(List<WritableShreddedDocument> documents) {
    final int count = documents.size();
    List<CollectionInsertAttempt> result = new ArrayList<>(count);
    for (int i = 0; i < count; ++i) {
      result.add(from(i, documents.get(i)));
    }
    return result;
  }

  @Override
  public int position() {
    return position;
  }

  @Override
  public Optional<DocRowIdentifer> docRowID() {
    return Optional.ofNullable(documentId);
  }

  @Override
  public Optional<WritableDocRow> docRow() {
    return Optional.ofNullable(document);
  }

  @Override
  public Optional<Throwable> failure() {
    return Optional.ofNullable(failure);
  }

  @Override
  public InsertAttempt maybeAddFailure(Throwable failure) {
    if (this.failure == null) {
      this.failure = failure;
    }
    return this;
  }

  public boolean hasVectorValues() {
    // TODO: AARON work out if we need hasVecotrs int he base on the base
    return (document != null) && (document.queryVectorValues() != null);
  }
}
