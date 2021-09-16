package org.traffic.traffic_registry.sensor;

import io.vertx.core.Future;

public interface SensorRepository {

  Future<String> save(Sensor sensor);
}
