package org.traffic.traffic_registry.point;

import io.vertx.core.Future;

public interface PointRepository {

  Future<String> save(Point point);

  Future<String> findById(String id);
}
