package org.traffic.traffic_registry.stream;

import io.vertx.core.json.JsonObject;
import lombok.Value;
import lombok.val;
import org.traffic.traffic_registry.point.Point;

@Value
public class Stream {

  String id;
  String derivedFrom;
  String generatedBy;
  Point location;

  public static Stream fromJson(JsonObject json) {
    return new Stream(
        json.getString("id"),
        json.getString("derivedFrom"),
        json.getString("generatedBy"),
        Point.fromJson(json.getJsonObject("location")));
  }

  public static JsonObject asJson(Stream stream) {
    val json = new JsonObject();
    json.put("id", stream.id);
    json.put("generatedBy", stream.generatedBy);
    if (stream.generatedBy != null) json.put("derivedFrom", stream.derivedFrom);
    json.put("location", Point.asJson(stream.location));
    return json;
  }
}
