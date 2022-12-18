package io.stargate.sgv3.docsapi.service.shredding.model;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
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
      case BOOLEAN -> booleanValue(value.booleanValue()).hashValue();
      case NULL -> nullValue().hashValue();
      case NUMBER -> numberValue(value.decimalValue()).hashValue();
      case OBJECT -> objectHash((ObjectNode) value);
      case STRING -> stringValue(value.textValue()).hashValue();

      default -> // case BINARY, MISSING, POJO
      throw new IllegalArgumentException("Unsupported JsonNodeType: " + value.getNodeType());
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

  private DocValueHash calcArrayHash(ArrayNode n) {
    // !!! TODO: proper implementation:

    // Actual implementation would actually use iterated over values;
    // we will traverse to exercise caching for tests but not really use:
    for (JsonNode element : n) {
      DocValueHash childHash = hash(element);
    }

    return new DocValueHash("[]");
  }

  private DocValueHash calcObjectHash(ObjectNode n) {
    // !!! TODO: proper implementation:

    // Actual implementation would actually use iterated over values;
    // we will traverse to exercise caching for tests but not really use:
    Iterator<Map.Entry<String, JsonNode>> it = n.fields();
    while (it.hasNext()) {
      Map.Entry<String, JsonNode> entry = it.next();
      DocValueHash childHash = hash(entry.getValue());
    }

    return new DocValueHash("{}");
  }
}
