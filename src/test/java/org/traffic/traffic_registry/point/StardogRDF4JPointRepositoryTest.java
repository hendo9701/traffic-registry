package org.traffic.traffic_registry.point;

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

@ExtendWith(VertxExtension.class)
@Slf4j
class StardogRDF4JPointRepositoryTest {

  private static JsonObject config;

  private static Point point;

  private static String rdfPoint;

  @BeforeAll
  static void prepare(Vertx vertx) {
    config = new JsonObject(vertx.fileSystem().readFileBlocking("points/point-repository.json"));
    point =
        Point.fromJson(new JsonObject(vertx.fileSystem().readFileBlocking("points/point.json")));
    rdfPoint = vertx.fileSystem().readFileBlocking("points/point.ttl").toString();

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
    val repository = new StardogRDF4JPointRepository(config);
    repository
        .save(point)
        .onComplete(
            testContext.succeeding(
                rdf ->
                    testContext.verify(
                        () -> {
                          log.debug("RDF: {}", rdf);
                          val givenModel = Rio.parse(new StringReader(rdf), RDFFormat.TURTLE);
                          val expectedModel =
                              Rio.parse(new StringReader(rdfPoint), RDFFormat.TURTLE);
                          Assertions.assertEquals(expectedModel, givenModel);
                          testContext.completeNow();
                        })));

    testContext.completeNow();
    Assertions.assertTrue(testContext.awaitCompletion(3, TimeUnit.SECONDS));

    if (testContext.failed()) throw testContext.causeOfFailure();
  }
}
