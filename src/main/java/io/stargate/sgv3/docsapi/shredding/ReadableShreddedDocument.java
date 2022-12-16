package io.stargate.sgv3.docsapi.shredding;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.javatuples.Pair;

/** Information needed only to read and project a document. */
public class ReadableShreddedDocument extends IDShreddedDocument {

  // tx_id can be null if the document is new, the tx_id is generated in the DB
  public final Optional<UUID> txID;
  public final List<JSONPath> docFieldOrder;
  public final Map<JSONPath, Pair<JsonType, ByteBuffer>> docAtomicFields;

  public ReadableShreddedDocument(
      String key,
      Optional<UUID> txID,
      List<JSONPath> docFieldOrder,
      Map<JSONPath, Pair<JsonType, ByteBuffer>> docAtomicFields) {
    super(key);
    this.docFieldOrder = docFieldOrder;
    this.docAtomicFields = docAtomicFields;
    this.txID = txID;
  }
}
