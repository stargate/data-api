package io.stargate.sgv2.jsonapi.service.operation.tables;

import com.datastax.oss.driver.api.querybuilder.select.Select;
import java.util.function.Function;

/** TODO: Still POC, working out what "other" options we need to be able to add to statements */
public interface OptionsBuilder extends Function<Select, Select> {}
