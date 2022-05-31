package com.mindarray;

import io.vertx.core.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class Bootstrap {

    public static final Vertx vertx = Vertx.vertx(new VertxOptions().setMaxEventLoopExecuteTime(3000).setMaxEventLoopExecuteTimeUnit(TimeUnit.MILLISECONDS));
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

        vertx.deployVerticle(verticle, handler -> {

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
