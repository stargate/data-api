package io.stargate.sgv2.jsonapi.service.operation.tables;

import com.datastax.oss.driver.api.querybuilder.insert.OngoingValues;
import com.datastax.oss.driver.api.querybuilder.insert.RegularInsert;

import java.util.List;
import java.util.function.BiFunction;

public interface InsertValuesBuilder extends BiFunction<OngoingValues, List<Object>, RegularInsert> {
}
