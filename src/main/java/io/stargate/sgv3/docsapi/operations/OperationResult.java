package io.stargate.sgv3.docsapi.operations;

import io.stargate.sgv3.docsapi.commands.Command;
import io.stargate.sgv3.docsapi.shredding.IDShreddedDocument;
import io.stargate.sgv3.docsapi.shredding.ReadableShreddedDocument;
import java.util.List;
import java.util.Optional;

/**
 * Operations return a result that may be documents, status about modified docs, or errors.
 *
 * <p>{@link OperationResult} is the return from {@link Operation}s back to {@link Command} which is
 * responsible for working out how to take the internal representation (shredded) back to the JSON
 * or whatever is needed for the response.
 *
 * <p>Operations do not touch JSON, so they return our Shredded document.
 *
 * <p>This is a dump POJO the basically has the super set of everything we could return for the
 * command.
 */
public class OperationResult {

  // Builder will make sure the lists are always set and empty if not supplied
  public final List<? extends ReadableShreddedDocument> docs;
  public final Optional<String> nextPageState;

  public final Optional<Integer> matchedCount;
  public final List<? extends IDShreddedDocument> insertedIds;
  public final List<? extends IDShreddedDocument> updatedIds;

  public final List<Exception> errors;
  public final boolean schemaChanged;

  private OperationResult(
      List<? extends ReadableShreddedDocument> docs,
      Optional<String> nextPageState,
      List<Exception> errors,
      Optional<Integer> matchedCount,
      List<? extends IDShreddedDocument> insertedIds,
      List<? extends IDShreddedDocument> updatedIds,
      boolean schemaChanged) {
    this.docs = docs;
    this.nextPageState = nextPageState;
    this.errors = errors;

    this.matchedCount = matchedCount;
    this.insertedIds = insertedIds;
    this.updatedIds = updatedIds;
    this.schemaChanged = schemaChanged;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private List<? extends ReadableShreddedDocument> docs;
    private Optional<String> nextPageState = Optional.empty();

    private Optional<Integer> matchedCount = Optional.empty();
    private List<? extends IDShreddedDocument> insertedIds;
    private List<? extends IDShreddedDocument> updatedIds;

    private boolean schemaChanged;

    private List<Exception> errors;

    Builder() {}

    Builder withSchemaChange(boolean schemaChanged) {
      this.schemaChanged = schemaChanged;
      return this;
    }

    Builder withDocs(List<? extends ReadableShreddedDocument> docs, String nextPageState) {
      this.docs = docs;
      this.nextPageState = Optional.ofNullable(nextPageState);
      return this;
    }

    Builder withErrors(List<Exception> errors) {
      this.errors = errors;
      return this;
    }

    Builder withMatchedCount(Integer matchedCount) {
      this.matchedCount = Optional.ofNullable(matchedCount);
      return this;
    }

    Builder withInsertedIds(List<? extends IDShreddedDocument> insertedIds) {
      this.insertedIds = insertedIds;
      return this;
    }

    Builder withUpdatedIds(List<? extends IDShreddedDocument> updatedIds) {
      this.updatedIds = updatedIds;
      return this;
    }

    OperationResult build() {

      // If there are errors nothing else gets through
      if (errors != null && !errors.isEmpty()) {
        return new OperationResult(
            List.of(), Optional.empty(), errors, Optional.empty(), List.of(), List.of(), false);
      }

      return new OperationResult(
          docs == null ? List.of() : docs,
          nextPageState,
          List.of(),
          matchedCount,
          insertedIds == null ? List.of() : insertedIds,
          updatedIds == null ? List.of() : updatedIds,
          schemaChanged);
    }
  }
}
