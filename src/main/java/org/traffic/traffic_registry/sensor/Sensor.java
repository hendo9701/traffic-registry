package org.traffic.traffic_registry.sensor;

import io.vertx.core.json.JsonObject;
import lombok.Value;

@Value
public class Sensor {

  String id;
  String quantityKind;
  String unit;

  public static Sensor fromJson(JsonObject json) {
    return new Sensor(json.getString("id"), json.getString("quantityKind"), json.getString("unit"));
  }
}
