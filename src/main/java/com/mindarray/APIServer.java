package com.mindarray;

import com.mindarray.api.Credentials;
import com.mindarray.api.Discovery;
import com.mindarray.api.Monitor;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.BodyHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class APIServer extends AbstractVerticle {
    private static final Logger LOGGER = LoggerFactory.getLogger(APIServer.class);

    @Override
    public void start(Promise<Void> startPromise) {
        LOGGER.debug("APISERVER DEPLOYED");

        Router router = Router.router(vertx);

        router.route().handler(BodyHandler.create());

        var discoveryRoute = Router.router(vertx);

        var credentialRoute = Router.router(vertx);

        var monitorRoute = Router.router(vertx);

        router.mountSubRouter(Constant.ROUTE_API, discoveryRoute);

        router.mountSubRouter(Constant.ROUTE_API, credentialRoute);

        router.mountSubRouter(Constant.ROUTE_API, monitorRoute);

        monitorRoute.route().handler(BodyHandler.create());

        credentialRoute.route().handler(BodyHandler.create());

        discoveryRoute.route().handler(BodyHandler.create());

        Discovery discovery = new Discovery();

        discovery.init(discoveryRoute);

        Credentials credentials = new Credentials();

        credentials.init(credentialRoute);

        Monitor monitor = new Monitor();

        monitor.init(monitorRoute);

        vertx.createHttpServer().requestHandler(router).listen(8888).onComplete(handler -> {

            if (handler.succeeded()) {

                LOGGER.debug("Server Created on port 8888");

            } else {

                LOGGER.debug("Server Failed");

            }

        });


        startPromise.complete();
    }
}
