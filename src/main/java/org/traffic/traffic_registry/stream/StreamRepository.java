package org.traffic.traffic_registry.stream;

import io.vertx.core.Future;

import static java.lang.String.format;

public interface StreamRepository {

  Future<String> save(Stream stream);

  Future<String> findById(String id);

  static String toLocalName(String id) {
    return format("/points/%s", id);
  }
}
