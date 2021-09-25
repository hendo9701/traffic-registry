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

import static org.traffic.traffic_registry.Vocabulary.*;
import static org.traffic.traffic_registry.sensor.SensorRepository.toLocalName;

public final class StardogRDF4JSensorRepository extends AbstractStardogRDFRepository
    implements SensorRepository {

  public StardogRDF4JSensorRepository(JsonObject config) {
    super(config);
  }

  @Override
  public Future<String> save(Sensor sensor) {

    val iri = Values.iri(namespace, toLocalName(sensor.getId()));

    val model =
        new ModelBuilder()
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
