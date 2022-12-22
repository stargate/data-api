package io.stargate.sgv3.docsapi.service.shredding.model;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.stargate.sgv3.docsapi.exception.DocsException;
import io.stargate.sgv3.docsapi.exception.ErrorCode;
import java.math.BigDecimal;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Helper class used to efficiently calculate {@link DocValueHash} on input documents as part of
 * shredding or query preparation.
 *
 * <p>Instances are stateful and not designed thread-safe: instances meant to be used from a single
 * thread, one instance per processing of a Command.
 */
public class DocValueHasher {
  private static final char LINE_SEPARATOR = '\n';

  /**
   * Simple reuse cache to avoid re-calculating hashes for nested Arrays and sub-documents. Not used
   * for atomic {@link JsonNode}s (they use cheaper map)
   */
  final IdentityHashMap<JsonNode, DocValueHash> structuredHashes = new IdentityHashMap<>();

  /**
   * Simple reuse cache for non-trivial atomic values: Strings and Numbers (nulls and booleans can
   * be pre-computed)
   */
  final AtomicValues atomics = new AtomicValues();

  public DocValueHash hash(JsonNode value) {
    return switch (value.getNodeType()) {
      case ARRAY -> arrayHash((ArrayNode) value);
      case BOOLEAN -> booleanValue(value.booleanValue()).hash();
      case NULL -> nullValue().hash();
      case NUMBER -> numberValue(value.decimalValue()).hash();
      case OBJECT -> objectHash((ObjectNode) value);
      case STRING -> stringValue(value.textValue()).hash();

      default -> // case BINARY, MISSING, POJO
      throw new DocsException(
          ErrorCode.SHRED_UNRECOGNIZED_NODE_TYPE,
          String.format(
              "%s: %s", ErrorCode.SHRED_UNRECOGNIZED_NODE_TYPE.getMessage(), value.getNodeType()));
    };
  }

  public AtomicValue booleanValue(boolean b) {
    return atomics.booleanValue(b);
  }

  public AtomicValue numberValue(BigDecimal value) {
    return atomics.numberValue(value);
  }

  public AtomicValue nullValue() {
    return atomics.nullValue();
  }

  public AtomicValue stringValue(String str) {
    return atomics.stringValue(str);
  }

  public DocValueHash arrayHash(ArrayNode n) {
    DocValueHash hash = structuredHashes.get(n);
    if (hash == null) {
      hash = calcArrayHash(n);
      structuredHashes.put(n, hash);
    }
    return hash;
  }

  public DocValueHash objectHash(ObjectNode n) {
    DocValueHash hash = structuredHashes.get(n);
    if (hash == null) {
      hash = calcObjectHash(n);
      structuredHashes.put(n, hash);
    }
    return hash;
  }

  /*
  /**********************************************************************
  /* Actual hash/digest calculation logic, structured
  /**********************************************************************
   */

  DocValueHash calcArrayHash(ArrayNode n) {
    // Array hash consists of header line (type prefix + element count)
    // followed by one line per element, containing element hash.
    // Lines are separated by linefeeds; no trailing linefeed
    StringBuilder sb = new StringBuilder(10 + 16 * n.size());

    // Header line: type prefix ('A') and element length, so f.ex "A13"
    sb.append(DocValueType.ARRAY.prefix()).append(n.size());

    for (JsonNode element : n) {
      DocValueHash childHash = hash(element);
      sb.append(LINE_SEPARATOR).append(childHash.hash());
    }

    return DocValueHash.constructBoundedHash(DocValueType.ARRAY, sb.toString());
  }

  private DocValueHash calcObjectHash(ObjectNode n) {
    // Array hash consists of header line (type prefix + element count)
    // followed by two line per element, containing name (NOT path!) on first line
    // and element hash on second.
    // Lines are separated by linefeeds; no trailing linefeed
    StringBuilder sb = new StringBuilder(10 + 24 * n.size());

    // Header line: type prefix ('O') and element length, so f.ex "O7"
    sb.append(DocValueType.OBJECT.prefix()).append(n.size());

    // Actual implementation would actually use iterated over values;
    // we will traverse to exercise caching for tests but not really use:
    Iterator<Map.Entry<String, JsonNode>> it = n.fields();
    while (it.hasNext()) {
      Map.Entry<String, JsonNode> entry = it.next();
      sb.append(LINE_SEPARATOR).append(entry.getKey());
      DocValueHash childHash = hash(entry.getValue());
      sb.append(LINE_SEPARATOR).append(childHash.hash());
    }

    return DocValueHash.constructBoundedHash(DocValueType.OBJECT, sb.toString());
  }
}
