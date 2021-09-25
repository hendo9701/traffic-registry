package org.traffic.traffic_registry;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.http.HttpServer;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.servicediscovery.Record;
import io.vertx.servicediscovery.ServiceDiscovery;
import io.vertx.servicediscovery.types.HttpEndpoint;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import static org.traffic.traffic_registry.sensor.SensorService.SENSOR_SERVICE_ADDRESS;
import static org.traffic.traffic_registry.stream.StreamService.STREAM_SERVICE_ADDRESS;

@Slf4j
@NoArgsConstructor
public final class RegistryVerticle extends AbstractVerticle {

  private HttpServer server;

  private ServiceDiscovery serviceDiscovery;

  private Record registryRecord;

  @Override
  public void start(Promise<Void> startPromise) throws Exception {

    val port = config().getInteger("port");
    val host = config().getString("host");
    val router = buildRouter();

    vertx
        .createHttpServer()
        .requestHandler(router)
        .listen(port)
        .compose(
            server -> {
              this.server = server;
              this.serviceDiscovery = ServiceDiscovery.create(vertx);
              val record = HttpEndpoint.createRecord("traffic-registry", host, port, "/api");
              return this.serviceDiscovery.publish(record);
            })
        .onSuccess(
            record -> {
              this.registryRecord = record;
              startPromise.complete();
            })
        .onFailure(
            cause -> {
              log.debug("Registry deployment failed");
              startPromise.fail(cause);
            });
  }

  @Override
  public void stop(Promise<Void> stopPromise) {
    serviceDiscovery
        .unpublish(registryRecord.getRegistration())
        .compose(__ -> server.close())
        .onComplete(
            result -> {
              serviceDiscovery.close();
              if (result.succeeded()) stopPromise.complete();
              else stopPromise.fail(result.cause());
            });
  }

  private Router buildRouter() {
    val router = Router.router(vertx);
    val bodyHandler = BodyHandler.create();

    val v1Router = Router.router(vertx);
    v1Router
        .post("/sensors")
        .consumes("application/json")
        .produces("text/turtle")
        .handler(bodyHandler)
        .handler(this::saveSensor);

    v1Router
        .post("/streams")
        .consumes("application/json")
        .produces("text/turtle")
        .handler(bodyHandler)
        .handler(this::saveStream);

    router.mountSubRouter("/api/v1", v1Router);
    return router;
  }

  private void saveSensor(RoutingContext routingContext) {
    val sensorJson = routingContext.getBodyAsJson();
    val action = new DeliveryOptions().addHeader("action", "save");
    vertx
        .eventBus()
        .<JsonObject>request(SENSOR_SERVICE_ADDRESS, sensorJson, action)
        .onSuccess(
            reply -> {
              val rdf = reply.body().getString("result");
              routingContext.response().setStatusCode(201).end(rdf);
            })
        .onFailure(
            throwable -> routingContext.response().setStatusCode(500).end(throwable.getMessage()));
  }

  private void saveStream(RoutingContext routingContext) {
    val streamJson = routingContext.getBodyAsJson();
    val action = new DeliveryOptions().addHeader("action", "save");
    vertx
        .eventBus()
        .<JsonObject>request(STREAM_SERVICE_ADDRESS, streamJson, action)
        .onSuccess(
            reply -> {
              val rdf = reply.body().getString("result");
              routingContext.response().setStatusCode(201).end(rdf);
            })
        .onFailure(
            throwable -> routingContext.response().setStatusCode(500).end(throwable.getMessage()));
  }
}
