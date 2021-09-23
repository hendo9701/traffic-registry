package org.traffic.traffic_registry.point;

import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.eclipse.rdf4j.model.util.ModelBuilder;
import org.eclipse.rdf4j.model.util.Values;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.query.QueryResults;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.Rio;
import org.traffic.traffic_registry.common.AbstractStardogRDFRepository;

import java.io.StringWriter;

import static java.lang.String.format;
import static org.traffic.traffic_registry.Vocabulary.*;

@Slf4j
public final class StardogRDF4JPointRepository extends AbstractStardogRDFRepository
    implements PointRepository {

  public StardogRDF4JPointRepository(JsonObject config) {
    super(config);
  }

  @Override
  public Future<String> save(Point point) {
    try (val connection = repository.getConnection()) {
      val pointIri = Values.iri(namespace, toLocalName(point.getId()));
      try (val statements = connection.getStatements(pointIri, null, null)) {
        val writer = new StringWriter();
        // Point does not exist
        if (!statements.hasNext()) {
          log.info("Saving point: [{}]", point);
          connection.begin();
          val graphIri = Values.iri(namespace, point.getId());
          val model =
              new ModelBuilder()
                  .namedGraph(graphIri)
                  .setNamespace(namespace)
                  .setNamespace(GEO)
                  .subject(pointIri)
                  .add(RDF.TYPE, POINT)
                  .add(LATITUDE, point.getLatitude())
                  .add(LONGITUDE, point.getLongitude())
                  .build();
          connection.add(model);
          connection.commit();
          Rio.write(model, writer, RDFFormat.TURTLE);
        } else {
          // Point does exist
          log.info("Point: [{}] already existed", point.getId());
          val model = QueryResults.asModel(statements);
          model.setNamespace(namespace);
          model.setNamespace(GEO);
          Rio.write(model, writer, RDFFormat.JSONLD);
        }
        return Future.succeededFuture(writer.toString());
      }
    } catch (Exception e) {
      return Future.failedFuture(e);
    }
  }

  @Override
  public Future<String> findById(String id) {
    try (val connection = repository.getConnection()) {
      val pointIri = Values.iri(namespace, toLocalName(id));
      try (val statements = connection.getStatements(pointIri, null, null)) {
        val writer = new StringWriter();
        // Point does not exist
        if (!statements.hasNext()) {
          return Future.failedFuture("Not found");
        } else {
          // Point does exist
          log.info("Point: [{}] already existed", id);
          val model = QueryResults.asModel(statements);
          Rio.write(model, writer, RDFFormat.TURTLE);
        }
        return Future.succeededFuture(writer.toString());
      }
    } catch (Exception e) {
      return Future.failedFuture(e);
    }
  }

  @Override
  public String toLocalName(String id) {
    return format("/points/%s", id);
  }
}
