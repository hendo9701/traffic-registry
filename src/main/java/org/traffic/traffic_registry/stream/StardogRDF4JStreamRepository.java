package org.traffic.traffic_registry.stream;

import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import org.traffic.traffic_registry.common.AbstractStardogRDFRepository;

import static java.lang.String.format;

public final class StardogRDF4JStreamRepository extends AbstractStardogRDFRepository
    implements StreamRepository {

  public StardogRDF4JStreamRepository(JsonObject config) {
    super(config);
  }

  @Override
  public String toLocalName(String id) {
    return format("/streams/%s", id);
  }

  @Override
  public Future<String> save(Stream stream) {
    return null;
  }
}
