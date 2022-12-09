package io.stargate.sgv3.docsapi.service.shredding;

import com.fasterxml.jackson.databind.JsonNode;
import io.stargate.sgv3.docsapi.service.shredding.model.WritableShreddedDocument;
import javax.enterprise.context.ApplicationScoped;
import javax.validation.constraints.NotNull;

/**
 * Shred an incoming JSON document into the data we need to store in the DB, and then de-shred.
 *
 * <p>This will be based on the ideas in the python lab, and extended to do things like make better
 * decisions about when to use a hash and when to use the actual value. i.e. a hash of "a" is a lot
 * longer than "a".
 */
@ApplicationScoped
public class Shredder {

  /**
   * Shreds a single JSON node into a {@link WritableShreddedDocument} representation.
   *
   * @param document {@link JsonNode} to shred.
   * @return WritableShreddedDocument
   */
  public WritableShreddedDocument shred(@NotNull JsonNode document) {
    // TODO @tatu implement me
    return new WritableShreddedDocument();
  }
}
