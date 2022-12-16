package io.stargate.sgv3.docsapi.service.shredding.model;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.IdentityHashMap;

/**
 * Helper class used to efficiently calculate {@link DocValueHash} on input documents as part of
 * shredding or query preparation.
 *
 * <p>Instances are stateful and not designed thread-safe: instances meant to be used from a single
 * thread, one instance per processing of a Command.
 */
public class DocValueHasher {
  static final DocValueHash HASH_NULL = new DocValueHash("null"); // !!! TODO

  static final DocValueHash HASH_BOOLEAN_FALSE = new DocValueHash("false"); // !!! TODO
  static final DocValueHash HASH_BOOLEAN_TRUE = new DocValueHash("true"); // !!! TODO

  /**
   * Simple reuse cache to avoid re-calculating hashes for nested Arrays and sub-documents. Not used
   * for atomic {@link JsonNode}s (they use cheaper map)
   */
  final IdentityHashMap<JsonNode, DocValueHash> structuredHashes = new IdentityHashMap<>();

  /**
   * Simple reuse cache for non-trivial atomic values: Strings and Numbers (nulls and booleans can
   * be pre-computed)
   */
  final HashMap<Object, DocValueHash> atomicHashes = new HashMap<>();

  public DocValueHash hash(JsonNode value) {
    return switch (value.getNodeType()) {
      case ARRAY -> arrayHash((ArrayNode) value);
      case BOOLEAN -> value.booleanValue() ? HASH_BOOLEAN_TRUE : HASH_BOOLEAN_FALSE;
      case NULL -> HASH_NULL;
      case NUMBER -> numberHash(value.decimalValue());
      case OBJECT -> objectHash((ObjectNode) value);
      case STRING -> stringHash(value.textValue());

      default -> // case BINARY, MISSING, POJO
      throw new IllegalArgumentException("Unsupported JsonNodeType: " + value.getNodeType());
    };
  }

  private DocValueHash numberHash(BigDecimal value) {
    DocValueHash hash = atomicHashes.get(value);
    if (hash == null) {
      hash = calcNumberHash(value);
      atomicHashes.put(value, hash);
    }
    return hash;
  }

  private DocValueHash stringHash(String value) {
    DocValueHash hash = atomicHashes.get(value);
    if (hash == null) {
      hash = calcStringHash(value);
      atomicHashes.put(value, hash);
    }
    return hash;
  }

  private DocValueHash arrayHash(ArrayNode n) {
    DocValueHash hash = structuredHashes.get(n);
    if (hash == null) {
      hash = calcArrayHash(n);
      structuredHashes.put(n, hash);
    }
    return hash;
  }

  private DocValueHash objectHash(ObjectNode n) {
    DocValueHash hash = structuredHashes.get(n);
    if (hash == null) {
      hash = calcObjectHash(n);
      structuredHashes.put(n, hash);
    }
    return hash;
  }

  /*
  /**********************************************************************
  /* Actual hash/digest calculation logic, atomics
  /**********************************************************************
   */

  private DocValueHash calcNumberHash(BigDecimal value) {
    return new DocValueHash(value.toPlainString()); // !!! TODO
  }

  private DocValueHash calcStringHash(String value) {
    return new DocValueHash(value); // !!! TODO
  }

  /*
  /**********************************************************************
  /* Actual hash/digest calculation logic, structured
  /**********************************************************************
   */

  private DocValueHash calcArrayHash(ArrayNode n) {
    return new DocValueHash("[]"); // !!! TODO
  }

  private DocValueHash calcObjectHash(ObjectNode n) {
    return new DocValueHash("{}"); // !!! TODO
  }
}
