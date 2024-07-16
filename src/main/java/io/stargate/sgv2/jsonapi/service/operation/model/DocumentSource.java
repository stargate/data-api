package io.stargate.sgv2.jsonapi.service.operation.model;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.function.Supplier;

/**
 * A source of document that have been read from the DB, the document could be from a collection or
 * a table.
 *
 * <p>The implementation may create the {@link JsonNode} lazy when it is called, or create and
 * cache.
 *
 * <p>Note: this does not implement {@link Supplier} because there are times we use these and
 * subclasses in a Uni chain which will see the supplier and call it, when what we really want to do
 * is pass the instance along.
 */
@FunctionalInterface
public interface DocumentSource {

  /**
   * Get the document as a JsonNode
   *
   * <p>Note to implementers, this method must always return a document. It is not valid to return
   * null.
   *
   * @return the document as a JsonNode
   */
  JsonNode get();
}
