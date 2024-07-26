package io.stargate.sgv2.jsonapi.service.operation.tables;

import com.datastax.oss.driver.api.querybuilder.select.Select;


import java.util.function.Function;

public interface OptionsBuilder extends Function<Select, Select>{

}
