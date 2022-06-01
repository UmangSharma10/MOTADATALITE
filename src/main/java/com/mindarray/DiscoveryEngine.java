package com.mindarray;

import com.mindarray.utility.Utility;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.mindarray.Constant.*;

public class DiscoveryEngine extends AbstractVerticle {
    private static final Logger LOGGER = LoggerFactory.getLogger(DiscoveryEngine.class);

    @Override
    public void start(Promise<Void> startPromise) {

        LOGGER.debug("DISCOVERY ENGINE DEPLOYED");

        vertx.eventBus().<JsonObject>consumer(Constant.EVENTBUS_DISCOVERY, handler -> {

            JsonObject discoveryCredentials = handler.body();

            Bootstrap.vertx.<JsonObject>executeBlocking(event -> {

                try {

                    JsonObject result = Utility.pingAvailability(discoveryCredentials.getString(Constant.IP_ADDRESS));

                    if (result.getString(Constant.STATUS).equals(Constant.UP)) {

                        JsonObject discoveryResult = Utility.spawning(discoveryCredentials);

                        if (discoveryResult.getString(Constant.STATUS).equals(Constant.SUCCESS)) {

                            discoveryCredentials.mergeIn(discoveryResult);

                            event.complete(discoveryCredentials);

                        } else {

                            event.fail(discoveryResult.encode());

                        }

                    } else if (result.getString(Constant.STATUS).equals(Constant.DOWN)) {

                        event.fail(result.encode());
                    }

                } catch (Exception exception) {

                    LOGGER.error(exception.getMessage());

                }

            }).onComplete(resultHandler -> {
                JsonObject result = new JsonObject();

                if (resultHandler.succeeded()) {

                    JsonObject discoveryData = resultHandler.result();

                    discoveryData.put(METHOD, EVENTBUS_UPDATE_DISCOVERYMETRIC);

                    if (!discoveryData.containsKey(ERROR)) {

                        vertx.eventBus().request(EVENTBUS_DATABASE, discoveryData, updateDisMet -> {

                            if (updateDisMet.succeeded()) {

                                result.put(Constant.STATUS, Constant.SUCCESS);

                                result.put(DISCOVERY, Constant.SUCCESS);

                                handler.reply(result);
                            } else {

                                String resultData = updateDisMet.cause().getMessage();

                                result.put(Constant.STATUS, Constant.FAILED);

                                result.put(DISCOVERY, Constant.FAILED);

                                result.put(Constant.ERROR, resultData + ", Ping Failed");

                                handler.fail(-1, resultData);
                            }
                        });
                    }
                } else {

                    String resultData = resultHandler.cause().getMessage();

                    result.put(Constant.STATUS, Constant.FAILED);

                    result.put(DISCOVERY, Constant.FAILED);

                    result.put(Constant.ERROR, resultData);

                    handler.fail(-1, result.encode());
                }

            });

        });

        startPromise.complete();

    }

}