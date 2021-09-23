package org.traffic.traffic_registry.stream;

import io.vertx.core.Future;

public interface StreamRepository {

  Future<String> save(Stream stream);
}
