package io.stargate.sgv2.jsonapi.service.cqldriver;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import io.quarkus.security.UnauthorizedException;
import io.stargate.sgv2.jsonapi.config.DatabaseType;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import java.util.Base64;
import java.util.List;
import org.junit.jupiter.api.Test;

/** Tests for {@link CqlCredentials.CqlCredentialsFactory}. */
public class CqlCredentialsFactoryTests {

  private static final String FIXED_TOKEN = "fixed-token";
  private static final String FIXED_USER_NAME = "fixed-user-name";
  private static final String FIXED_PASSWORD = "fixed-password";

  @Test
  public void authTokenMustMatchFixedToken() {

    var fixture = fixtureWithFixed();
    assertThrows(
        UnauthorizedException.class,
        () -> {
          fixture.factory.apply(FIXED_TOKEN + "wrong");
        });
  }

  @Test
  public void fixedTokenCreatesUsernamePasswordCreds() {
    // this happens even if the DB type is astra
    var fixture = fixtureWithFixed();
    var creds = fixture.factory.apply(FIXED_TOKEN);

    assertThat(creds)
        .as("Fixed token creates username/password credentials")
        .isInstanceOf(CqlCredentials.UsernamePasswordCredentials.class);

    assertUsernamePassword(
        (CqlCredentials.UsernamePasswordCredentials) creds, FIXED_USER_NAME, FIXED_PASSWORD);
    assertAddToSessionBuilder(creds, FIXED_USER_NAME, FIXED_PASSWORD, false);
  }

  @Test
  public void missingTokenAllowedForOffline() {

    // this is only allowed with the OFFLINE WRITER type
    var fixture1 = newFixture(null, null, null, DatabaseType.OFFLINE_WRITER);
    var creds1 = fixture1.factory.apply(null);

    assertIsAnonymous(creds1);
    assertAddToSessionBuilder(creds1, null, null, true);

    var fixture2 = newFixture(null, null, null, DatabaseType.OFFLINE_WRITER);
    var creds2 = fixture2.factory.apply("");

    assertIsAnonymous(creds2);
    assertAddToSessionBuilder(creds2, null, null, true);
  }

  @Test
  public void missingTokenErrorsForOnline() {

    var dbTypes = List.of(DatabaseType.ASTRA, DatabaseType.CASSANDRA);
    var tokens = new String[] {null, ""};

    for (var dbType : dbTypes) {
      for (var token : tokens) {
        var fixture = newFixture(null, null, null, dbType);

        JsonApiException ex =
            assertThrows(
                JsonApiException.class,
                () -> {
                  fixture.factory.apply(token);
                });

        assertThat(ex)
            .as("Exception message for dbType=%s, token='%s'", dbType, token)
            .hasMessageContaining(
                "Server internal error: Missing/Invalid authentication credentials provided for type: "
                    + dbType);
      }
    }
  }

  @Test
  public void cassandraTokenDecoded() {
    var userName = "username-1";
    var password = "password-1";

    var encodedUserName = Base64.getEncoder().encodeToString(userName.getBytes());
    var encodedPassword = Base64.getEncoder().encodeToString(password.getBytes());
    var authToken = "Cassandra:%s:%s".formatted(encodedUserName, encodedPassword);

    var fixture = fixtureWithoutFixed();
    var creds = fixture.factory.apply(authToken);

    assertThat(creds)
        .as("Cassandra token creates username/password credentials")
        .isInstanceOf(CqlCredentials.UsernamePasswordCredentials.class);

    assertUsernamePassword((CqlCredentials.UsernamePasswordCredentials) creds, userName, password);
    assertAddToSessionBuilder(creds, userName, password, false);
  }

  @Test
  public void astraTokenCreatesTokenCreds() {

    var authToken = "AstraCS:1234567890";

    var fixture = fixtureWithoutFixed();
    var creds = fixture.factory.apply(authToken);

    assertThat(creds)
        .as("Astra token creates token credentials")
        .isInstanceOf(CqlCredentials.TokenCredentials.class);
    assertThat(creds.isAnonymous()).as("Credentials is not anonymous").isFalse();

    assertAddToSessionBuilder(creds, "token", authToken, false);
  }

  private void assertIsAnonymous(CqlCredentials creds) {

    assertThat(creds)
        .as("Credentials are AnonymousCredentials")
        .isInstanceOf(CqlCredentials.AnonymousCredentials.class);

    assertThat(creds.isAnonymous()).as("Credentials isAnonymous").isTrue();
  }

  private void assertAddToSessionBuilder(
      CqlCredentials creds,
      String expectedUsername,
      String expectedPassword,
      boolean noInteractions) {

    var builder = mock(CqlSessionBuilder.class);
    when(builder.withAuthCredentials(any(), any())).thenReturn(builder);

    creds.addToSessionBuilder(builder);

    if (noInteractions) {
      verifyNoInteractions(builder);
    } else {
      verify(builder).withAuthCredentials(expectedUsername, expectedPassword);
      verifyNoMoreInteractions(builder);
    }
  }

  private void assertUsernamePassword(
      CqlCredentials.UsernamePasswordCredentials creds,
      String expectedUsername,
      String expectedPassword) {

    assertThat(creds.userName())
        .as("UsernamePasswordCredentials has the expected username")
        .isEqualTo(expectedUsername);

    assertThat(creds.password())
        .as("UsernamePasswordCredentials has the expected password")
        .isEqualTo(expectedPassword);

    assertThat(creds.isAnonymous()).as("Credentials is not anonymous").isFalse();
  }
  ;

  record Fixture(
      String fixedToken,
      String fixedUserName,
      String fixedPassword,
      CqlCredentials.CqlCredentialsFactory factory) {}
  ;

  private Fixture fixtureWithFixed() {
    return newFixture(FIXED_TOKEN, FIXED_USER_NAME, FIXED_PASSWORD, DatabaseType.ASTRA);
  }

  private Fixture fixtureWithoutFixed() {
    return newFixture(null, null, null, DatabaseType.ASTRA);
  }

  private Fixture newFixture(
      String fixedToken, String fixedUserName, String fixedPassword, DatabaseType databseType) {
    return new Fixture(
        fixedToken,
        fixedUserName,
        fixedPassword,
        new CqlCredentials.CqlCredentialsFactory(
            fixedToken, fixedUserName, fixedPassword, databseType));
  }
}
