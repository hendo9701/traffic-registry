package org.traffic.traffic_registry.sensor;

import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import lombok.val;
import org.eclipse.rdf4j.model.util.ModelBuilder;
import org.eclipse.rdf4j.model.util.Values;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.Rio;
import org.traffic.traffic_registry.common.AbstractStardogRDFRepository;

import java.io.StringWriter;

import static java.lang.String.format;
import static org.traffic.traffic_registry.Vocabulary.*;

public final class StardogRDF4JSensorRepository extends AbstractStardogRDFRepository
    implements SensorRepository {

  public StardogRDF4JSensorRepository(JsonObject config) {
    super(config);
  }

  @Override
  public String toLocalName(String id) {
    return format("/sensors/%s", id);
  }

  @Override
  public Future<String> save(Sensor sensor) {

    val iri = Values.iri(namespace, toLocalName(sensor.getId()));
    val graphIri = Values.iri(namespace, sensor.getId());

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
