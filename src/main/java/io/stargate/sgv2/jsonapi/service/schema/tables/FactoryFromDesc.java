package io.stargate.sgv2.jsonapi.service.schema.tables;

import static io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil.cqlIdentifierFromUserInput;

import com.datastax.oss.driver.api.core.CqlIdentifier;

/** Base for Factories that create api "Def" objects from the user inputted "Desc" objects. */
public abstract class FactoryFromDesc {

  protected CqlIdentifier userNameToIdentifier(String userName, String context) {
    if (userName == null) {
      throw new IllegalArgumentException("%s is must not be null".formatted(context));
    }
    if (userName.isBlank()) {
      throw new IllegalArgumentException("%s is must not be blank".formatted(context));
    }
    return cqlIdentifierFromUserInput(userName);
  }
}
