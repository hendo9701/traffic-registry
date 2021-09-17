package org.traffic.traffic_registry.sensor;

import static java.lang.String.format;
import static org.traffic.traffic_registry.Vocabulary.HAS_QUANTITY_KIND;
import static org.traffic.traffic_registry.Vocabulary.HAS_UNIT;
import static org.traffic.traffic_registry.Vocabulary.IOT_LITE;
import static org.traffic.traffic_registry.Vocabulary.QU;
import static org.traffic.traffic_registry.Vocabulary.SENSOR;
import static org.traffic.traffic_registry.Vocabulary.SOSA;

import com.complexible.stardog.api.ConnectionConfiguration;
import com.complexible.stardog.api.admin.AdminConnectionConfiguration;
import com.complexible.stardog.rdf4j.StardogRepository;
import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import java.io.StringWriter;
import lombok.val;
import org.eclipse.rdf4j.model.Namespace;
import org.eclipse.rdf4j.model.util.ModelBuilder;
import org.eclipse.rdf4j.model.util.Values;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.Rio;

public final class StardogRDF4JSensorRepository implements SensorRepository {

  private final Repository repository;

  private final Namespace trafficNamespace;

  public StardogRDF4JSensorRepository(Repository repository, String prefix, String namespace) {
    this.trafficNamespace = Values.namespace(prefix, namespace);
    this.repository = repository;
  }

  public StardogRDF4JSensorRepository(JsonObject config) {

    val databaseName = config.getString("database-name");
    val databaseUrl = config.getString("database-url");
    val server = config.getString("server");
    val username = config.getString("username");
    val password = config.getString("password");

    try (val adminConnection =
        AdminConnectionConfiguration.toServer(server).credentials(username, password).connect()) {
      if (!adminConnection.list().contains(databaseName)) {
        adminConnection.newDatabase(databaseName).create();
      }
    }

    this.repository =
        new StardogRepository(
            ConnectionConfiguration.from(databaseUrl).credentials(username, password));

    if (!this.repository.isInitialized()) this.repository.initialize();

    val namespace = config.getString("namespace");
    val prefix = config.getString("prefix");

    this.trafficNamespace = Values.namespace(prefix, namespace);
  }

  private static String toLocalName(String id) {
    return format("/sensors/%s", id);
  }

  @Override
  public Future<String> save(Sensor sensor) {

    val iri = Values.iri(trafficNamespace, toLocalName(sensor.getId()));
    val graphIri = Values.iri(trafficNamespace, sensor.getId());

    val model =
        new ModelBuilder()
            .namedGraph(graphIri)
            .setNamespace(SOSA)
            .setNamespace(IOT_LITE)
            .setNamespace(QU)
            .subject(iri)
            .add(RDF.TYPE, SENSOR)
            .add(HAS_QUANTITY_KIND, sensor.getQuantityKind())
            .add(HAS_UNIT, sensor.getUnit())
            .build();

    try (val connection = repository.getConnection()) {
      connection.begin();
      connection.add(model);
      connection.commit();
      val rdf = new StringWriter();
      Rio.write(model, rdf, RDFFormat.TURTLE);
      return Future.succeededFuture(rdf.toString());
    } catch (Exception e) {
      return Future.failedFuture(e);
    }
  }

  public void shutdown() {
    repository.shutDown();
  }
}
