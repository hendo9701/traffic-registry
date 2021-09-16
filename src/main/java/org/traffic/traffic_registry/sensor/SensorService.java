package org.traffic.traffic_registry.sensor;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Context;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.json.JsonObject;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import static java.lang.String.format;

@Slf4j
public final class SensorService extends AbstractVerticle {

  public static final String SENSOR_SERVICE_ADDRESS = "registry.sensor-service";

  private final SensorRepository sensorRepository;

  private MessageConsumer<JsonObject> consumer;

  public SensorService(SensorRepository sensorRepository) {
    this.sensorRepository = sensorRepository;
  }

  @Override
  public void init(Vertx vertx, Context context) {
    super.init(vertx, context);
  }

  @Override
  public void start(Promise<Void> startPromise) {
    consumer =
        vertx
            .eventBus()
            .consumer(
                SENSOR_SERVICE_ADDRESS,
                message -> {
                  switch (message.headers().get("action")) {
                    case "save":
                      save(message);
                      break;
                    default:
                      message.fail(
                          400, format("Unknown action: [%s]", message.headers().get("action")));
                  }
                });
  }

  private void save(Message<JsonObject> message) {
    val sensor = Sensor.fromJson(message.body());
    sensorRepository
        .save(sensor)
        .onSuccess(
            sensorGraph -> {
              log.info("Successfully inserted sensor: {}", sensor);
              message.reply(new JsonObject().put("result", sensorGraph));
            })
        .onFailure(
            throwable -> {
              throwable.printStackTrace();
              message.fail(500, format("Unable to save sensor: [%s]", sensor.getId()));
            });
  }

  @Override
  public void stop(Promise<Void> stopPromise) {
    consumer.unregister(stopPromise);
  }
}
