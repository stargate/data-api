package io.stargate.sgv2.jsonapi.service.operation.collections;

import com.fasterxml.jackson.databind.JsonNode;
import io.stargate.sgv2.jsonapi.service.operation.InsertAttemptBuilder;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionSchemaObject;
import io.stargate.sgv2.jsonapi.service.shredding.collections.DocumentId;
import io.stargate.sgv2.jsonapi.service.shredding.collections.DocumentShredder;
import io.stargate.sgv2.jsonapi.service.shredding.collections.WritableShreddedDocument;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

public class CollectionInsertAttemptBuilder
    implements InsertAttemptBuilder<CollectionInsertAttempt> {

  private final DocumentShredder documentShredder;
  private final CollectionSchemaObject collectionSchemaObject;

  // TODO: remove commandName - we only need commandName for handing to the shredder to report
  // metics
  private final String commandName;

  // starts at 0, we inc before use
  private int insertPosition = -1;

  public CollectionInsertAttemptBuilder(
      CollectionSchemaObject collectionSchemaObject,
      DocumentShredder documentShredder,
      String commandName) {
    this.collectionSchemaObject =
        Objects.requireNonNull(collectionSchemaObject, "collectionSchemaObject must not be null");
    this.documentShredder =
        Objects.requireNonNull(documentShredder, "documentShredder must not be null");
    this.commandName = commandName;
  }

  @Override
  public CollectionInsertAttempt build(JsonNode jsonNode) {

    WritableShreddedDocument shreddedDocument = null;
    Exception exception = null;
    AtomicReference<DocumentId> docIdRef = new AtomicReference<>();

    try {
      shreddedDocument =
          documentShredder.shred(
              jsonNode,
              null,
              collectionSchemaObject.indexingProjector(),
              commandName,
              collectionSchemaObject,
              docIdRef);
    } catch (RuntimeException e) {
      exception = e;
    }

    insertPosition += 1;
    var docId = docIdRef.get();
    var attempt =
        new CollectionInsertAttempt(
            collectionSchemaObject, insertPosition, docId, shreddedDocument);
    // OK to always call maybeAddFailure, if the exception is null it will be ignored
    return (CollectionInsertAttempt) attempt.maybeAddFailure(exception);
  }
}
