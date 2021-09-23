package org.traffic.traffic_registry.point;

import io.vertx.core.json.JsonObject;
import lombok.Value;

@Value
public class Point {

  String id;
  double latitude;
  double longitude;

  public static JsonObject asJson(Point point) {
    return new JsonObject()
        .put("id", point.id)
        .put("latitude", point.latitude)
        .put("longitude", point.longitude);
  }

  public static Point fromJson(JsonObject json) {
    return new Point(json.getString("id"), json.getDouble("latitude"), json.getDouble("longitude"));
  }
}
