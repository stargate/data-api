package io.stargate.sgv3.docsapi.service.shredding.model;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.javatuples.Pair;

/** The fully shredded document, everything we need to write the document. */
public record WritableShreddedDocument(
    String key,
    Optional<UUID> txID,
    List<JsonPath> docFieldOrder,
    Map<JsonPath, Pair<JsonType, ByteBuffer>> docAtomicFields,
    Map<JsonPath, Integer> properties,
    Set<JsonPath> existKeys,
    Map<JsonPath, String> subDocEquals,
    Map<JsonPath, Integer> arraySize,
    Map<JsonPath, String> arrayEquals,
    Map<JsonPath, String> arrayContains,
    Map<JsonPath, Boolean> queryBoolValues,
    Map<JsonPath, BigDecimal> queryNumberValues,
    Map<JsonPath, String> queryTextValues,
    Set<JsonPath> queryNullValues) {}
