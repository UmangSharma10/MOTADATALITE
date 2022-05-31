package com.mindarray;

import com.mindarray.utility.Utility;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;

public class PollingEngine extends AbstractVerticle {

    private static final Logger LOGGER = LoggerFactory.getLogger(PollingEngine.class);

    HashMap<Long, String> statusCheck = new HashMap<>();

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
                                vertx.eventBus().request(Constant.EVENTBUS_DATADUMP, result, dataDump -> {
                                    if (dataDump.succeeded()) {
                                        LOGGER.debug(value.getString(Constant.IP_ADDRESS) + " -> DATA DUMPED");
                                        context.complete();
                                    } else {
                                        LOGGER.debug(value.getString(Constant.IP_ADDRESS) + " -> DATA NOT DUMPED");
                                        context.fail(value.getString(Constant.IP_ADDRESS) + " -> DATA NOT DUMP");
                                    }
                                });
                            } else {
                                LOGGER.debug(value.getString(Constant.IP_ADDRESS) + " -> DATA NOT DUMPED");
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
