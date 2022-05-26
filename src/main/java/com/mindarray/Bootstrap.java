package com.mindarray;

import io.vertx.core.DeploymentOptions;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class Bootstrap {

    public static final Vertx vertx = Vertx.vertx();
    private static final Logger LOGGER = LoggerFactory.getLogger(Bootstrap.class);

    public static void main(String[] args) {


        start(APIServer.class.getName())

                .compose(future -> start(DatabaseEngine.class.getName()))

                .compose(future -> start(DiscoveryEngine.class.getName()))

                .compose(future -> start(SchedulingEngine.class.getName()))

                .compose(future -> start(PollingEngine.class.getName()))

                .onComplete(handler -> {

                    if (handler.succeeded()) {

                        LOGGER.debug("ALL VERTICLES DEPLOYED");

                    } else {
                        LOGGER.error("ERROR IN DEPLOYING");
                    }

                });
    }

    public static Future<Void> start(String verticle) {

        Promise<Void> promise = Promise.promise();

        vertx.deployVerticle(verticle, new DeploymentOptions().setMaxWorkerExecuteTime(4000).setMaxWorkerExecuteTimeUnit(TimeUnit.MILLISECONDS), handler -> {

            if (handler.succeeded()) {

                promise.complete();

            } else {
                LOGGER.debug(handler.cause().getMessage());

                promise.fail(handler.cause());

            }
        });
        return promise.future();
    }


}
