package io.stargate.sgv2.jsonapi.service.schema.tables;

import static io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil.cqlIdentifierFromUserInput;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import java.util.Objects;

public abstract class UserDescFactory {

  protected CqlIdentifier userNameToIdentifier(String userName, String context) {
    Objects.requireNonNull(userName, "%s is must not be null".formatted(context));
    if (userName.isBlank()) {
      throw new IllegalArgumentException("%s is must not be blank".formatted(context));
    }
    return cqlIdentifierFromUserInput(userName);
  }
}
