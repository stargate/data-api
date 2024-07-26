package io.stargate.sgv2.jsonapi.service.operation.tables;

import com.datastax.oss.driver.api.querybuilder.select.Select;

import java.util.List;
import java.util.function.BiFunction;

public interface WhereBuilder extends BiFunction<Select, List<Object>, Select>{
}
