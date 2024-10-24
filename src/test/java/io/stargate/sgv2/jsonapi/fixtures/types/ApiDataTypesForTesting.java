package io.stargate.sgv2.jsonapi.fixtures.types;

import static io.stargate.sgv2.jsonapi.service.schema.tables.ApiDataTypeDefs.*;

import io.stargate.sgv2.jsonapi.service.schema.tables.PrimitiveApiDataTypeDef;
import java.util.List;

public abstract class ApiDataTypesForTesting {

  public static final List<PrimitiveApiDataTypeDef> ALL_SCALAR_TYPES_FOR_CREATE =
      List.of(
          ASCII, BIGINT, BOOLEAN, BINARY, DATE, DECIMAL, DOUBLE, DURATION, FLOAT, INT, SMALLINT,
          TEXT, TIME, TIMESTAMP, TINYINT, VARINT, INET, UUID);
}
