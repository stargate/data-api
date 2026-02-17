package io.stargate.sgv2.jsonapi.service.operation.collections;

import com.datastax.oss.driver.api.querybuilder.insert.OngoingValues;
import com.datastax.oss.driver.api.querybuilder.insert.RegularInsert;
import io.stargate.sgv2.jsonapi.service.operation.InsertAttempt;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionSchemaObject;
import io.stargate.sgv2.jsonapi.service.shredding.DocRowIdentifer;
import io.stargate.sgv2.jsonapi.service.shredding.collections.DocumentId;
import io.stargate.sgv2.jsonapi.service.shredding.collections.WritableShreddedDocument;
import java.util.List;
import java.util.Optional;
import org.apache.commons.lang3.NotImplementedException;

/**
 * Container for an individual Document insertion attempt: used to keep track of the original input
 * position; document (if available), its id (if available) and possible processing error.
 * Information will be needed to build optional detail response (returnDocumentResponses).
 */
public class CollectionInsertAttempt extends InsertAttempt<CollectionSchemaObject> {

  // TODO: AARON moved out of the inner class to make sure the encapsulation was correct, previously
  // InsertCollectionOperation
  // was using private fields from this class

  public final WritableShreddedDocument document;
  private final DocumentId documentId;

  //  private Throwable failure;

  public CollectionInsertAttempt(
      CollectionSchemaObject collectionSchemaObject,
      int position,
      DocumentId documentId,
      WritableShreddedDocument document) {
    super(position, collectionSchemaObject, null);

    this.documentId = documentId;
    this.document = document;
    setStatus(OperationStatus.READY);
  }

  @Override
  protected RegularInsert applyInsertValues(
      OngoingValues ongoingValues, List<Object> positionalValues) {
    // aaron - collections does not support this pathway yet
    throw new NotImplementedException("CollectionInsertAttempt.getInsertValuesCQLClause()");
  }

  @Override
  public Optional<DocRowIdentifer> docRowID() {
    return Optional.ofNullable(documentId);
  }

  public boolean hasVectorValues() {
    // TODO: AARON work out if we need hasVectors in the base
    return (document != null) && (document.queryVectorValues() != null);
  }
}
