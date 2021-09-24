package org.traffic.traffic_registry.stream;

import com.complexible.stardog.api.admin.AdminConnectionConfiguration;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.Rio;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.StringReader;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(VertxExtension.class)
@Slf4j
class StardogRDF4JStreamRepositoryTest {

  private static Stream stream;

  private static JsonObject config;

  private static String rdfStream;

  @BeforeAll
  static void prepare(Vertx vertx) {
    stream =
        Stream.fromJson(new JsonObject(vertx.fileSystem().readFileBlocking("streams/stream.json")));
    config = new JsonObject(vertx.fileSystem().readFileBlocking("streams/stream-repository.json"));
    rdfStream = vertx.fileSystem().readFileBlocking("streams/stream.ttl").toString();
    val databaseName = config.getString("database-name");
    val server = config.getString("server");
    val username = config.getString("username");
    val password = config.getString("password");

    try (val adminConnection =
        AdminConnectionConfiguration.toServer(server).credentials(username, password).connect()) {
      if (adminConnection.list().contains(databaseName)) {
        adminConnection.drop(databaseName);
      }
    }
  }

  @Test
  void saveTest(VertxTestContext testContext) throws Throwable {
    val repository = new StardogRDF4JStreamRepository(config);
    repository
        .save(stream)
        .onComplete(
            testContext.succeeding(
                rdf ->
                    testContext.verify(
                        () -> {
                          val givenModel = Rio.parse(new StringReader(rdf), RDFFormat.TURTLE);
                          val expectedModel =
                              Rio.parse(new StringReader(rdfStream), RDFFormat.TURTLE);
                          log.debug("Given stream as RDF: {}", rdf);
                          Assertions.assertEquals(expectedModel, givenModel);
                          testContext.completeNow();
                        })));

    assertTrue(testContext.awaitCompletion(5, TimeUnit.SECONDS));

    if (testContext.failed()) {
      throw testContext.causeOfFailure();
    }
  }
}
