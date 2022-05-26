package com.mindarray;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class SchedulingEngine extends AbstractVerticle {
    private static final Logger LOGGER = LoggerFactory.getLogger(SchedulingEngine.class);

    @Override
    public void start(Promise<Void> startPromise) {
        LOGGER.debug("POLLER ENGINE DEPLOYED");

        HashMap<String, JsonObject> orginalData = new HashMap<>();

        HashMap<String, JsonObject> schedulingData = new HashMap<>();


        vertx.eventBus().<JsonObject>request(Constant.EVENTBUS_PRE_POLLING, new JsonObject(), getData -> {
            if (getData.succeeded()) {
                JsonObject entries = getData.result().body();
                entries.stream().forEach((key) -> {
                    var object = entries.getJsonObject(key.getKey());
                    orginalData.put(object.getString(Constant.IPANDGROUP), object);

                    schedulingData.put(object.getString(Constant.IPANDGROUP), object);

                });
            } else {
                LOGGER.debug(getData.cause().getMessage());
            }
        });

        vertx.eventBus().<JsonObject>consumer(Constant.EVENTBUS_POLLING, polHandler -> {

            JsonObject pollingData = polHandler.body();

            Bootstrap.vertx.eventBus().<JsonObject>request(Constant.EVENTBUS_GETMETRIC_FOR_POLLING, pollingData, getData -> {

                JsonObject result = new JsonObject();
                if (getData.succeeded()) {

                    JsonObject entries = getData.result().body();

                    entries.stream().forEach((key) -> {
                        var object = entries.getJsonObject(key.getKey());

                        orginalData.put(object.getString(Constant.IPANDGROUP), object);

                        schedulingData.put(object.getString(Constant.IPANDGROUP), object);

                        result.put(Constant.MONITOR_ID, object.getLong("monitorId"));
                    });


                    result.put(Constant.STATUS, Constant.SUCCESS);

                    polHandler.reply(result);

                } else {

                    polHandler.fail(-1, getData.cause().getMessage());

                }

            });

        });

        vertx.eventBus().<JsonObject>consumer(Constant.EVENTBUS_UPDATE_POLLING, updatePolling -> {
            JsonObject result = updatePolling.body();

            for (Map.Entry<String, JsonObject> entries : schedulingData.entrySet()) {
                if (entries.getValue().getString("monitorId").equals(result.getString(Constant.MONITOR_ID)) && entries.getValue().getString(Constant.METRIC_GROUP).equals(result.getString(Constant.METRIC_GROUP)) && entries.getValue().getString(Constant.METRIC_TYPE).equals(result.getString("metricType"))) {

                    entries.getValue().put(Constant.METRIC_GROUP, result.getString(Constant.METRIC_GROUP));
                    entries.getValue().put(Constant.METRIC_TYPE, result.getString("metricType"));
                    entries.getValue().put(Constant.TIME, result.getString("Time"));
                }
            }

            updatePolling.reply("Done");

        });



        Bootstrap.vertx.setPeriodic(10000, polhandling -> {

            for (Map.Entry<String, JsonObject> mapElement : schedulingData.entrySet()) {

                long time = mapElement.getValue().getLong(Constant.TIME);

                if (time <= 0) {

                    schedulingData.put(mapElement.getKey(), orginalData.get(mapElement.getKey()));

                        Bootstrap.vertx.eventBus().<JsonObject>request(Constant.EVENTBUS_POLLING_ENGINE, schedulingData.get(mapElement.getKey()), pollingHandler->{
                        if (pollingHandler.succeeded()){
                            LOGGER.info(pollingHandler.result().body().encode());
                        }
                        else {
                            LOGGER.info(pollingHandler.cause().getMessage());
                        }
                        });
                } else {
                    time = time - 10000;

                    schedulingData.put(mapElement.getKey(), mapElement.getValue().put(Constant.TIME, time));


                }
            }


        });

        startPromise.complete();
    }
}
