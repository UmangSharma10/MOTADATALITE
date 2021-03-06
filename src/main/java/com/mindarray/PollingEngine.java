package com.mindarray;

import com.mindarray.utility.Utility;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;

public class PollingEngine extends AbstractVerticle {

    private static final Logger LOGGER = LoggerFactory.getLogger(PollingEngine.class);

    private final ConcurrentHashMap<Long, String> statusCheck = new ConcurrentHashMap<>();

    @Override
    public void start(Promise<Void> startPromise) throws Exception {
        Bootstrap.vertx.eventBus().<JsonObject>localConsumer(Constant.EVENTBUS_POLLING_ENGINE, pollingEngineHandler -> {

            JsonObject value = pollingEngineHandler.body();
            if (value.getString(Constant.METRIC_GROUP).equals("ping")) {
                Bootstrap.vertx.executeBlocking(pollerBlocking -> {
                    try {
                        JsonObject pingResult = Utility.pingAvailability(value.getString(Constant.IP_ADDRESS));
                        if (pingResult.getString(Constant.STATUS).equals(Constant.UP)) {
                            statusCheck.put(value.getLong("monitorId"), pingResult.getString(Constant.STATUS));
                            pollerBlocking.complete(pingResult);
                        } else {
                            statusCheck.put(value.getLong("monitorId"), pingResult.getString(Constant.STATUS));
                            pollerBlocking.fail(new JsonObject().put("PING", Constant.FAILED).encode());
                        }

                    } catch (Exception exception) {
                        pollerBlocking.fail(new JsonObject().put("PING", Constant.FAILED).encode());
                    }
                });
            } else {
                if (!statusCheck.containsKey(value.getLong("monitorId")) || statusCheck.get(value.getLong("monitorId")).equals(Constant.UP)) {
                    Bootstrap.vertx.executeBlocking(context -> {
                        try {
                            JsonObject result = Utility.spawning(value);
                            if (!result.containsKey(Constant.ERROR)) {
                                vertx.eventBus().send(Constant.EVENTBUS_DATADUMP, result);
                                LOGGER.debug(value.getString(Constant.IP_ADDRESS) + " " + value.getString(Constant.METRIC_GROUP) + ": " + " -> DATA DUMPED");
                               context.complete("DATA DUMPED");
                            } else {
                                LOGGER.debug(value.getString(Constant.IP_ADDRESS) + " " + value.getString(Constant.METRIC_GROUP) + ": " + result.getString(Constant.ERROR) + " -> DATA NOT DUMPED");
                                context.fail("DATA NOT DUMP");
                            }

                        } catch (Exception exception) {
                            context.fail(exception.getMessage());
                        }
                    });
                }


            }
        });
        startPromise.complete();
    }
}
