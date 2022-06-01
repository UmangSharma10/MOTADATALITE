package com.mindarray;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;

public class SchedulingEngine extends AbstractVerticle {
    private static final Logger LOGGER = LoggerFactory.getLogger(SchedulingEngine.class);

    @Override
    public void start(Promise<Void> startPromise) {
        LOGGER.debug("POLLER ENGINE DEPLOYED");

        HashMap<String, Long> orginalData = new HashMap<>();

        HashMap<String, JsonObject> schedulingData = new HashMap<>();




        vertx.eventBus().<JsonObject>request(Constant.EVENTBUS_PRE_POLLING, new JsonObject(), getData -> {
            if (getData.succeeded()) {
                JsonObject entries = getData.result().body();
                entries.stream().forEach((key) -> {
                    var object = entries.getJsonObject(key.getKey());
                    orginalData.put(object.getString(Constant.IPANDGROUP), object.getLong(Constant.TIME));

                    schedulingData.put(object.getString(Constant.IPANDGROUP), object);

                });
            } else {
                LOGGER.debug(getData.cause().getMessage());
            }
        });

        vertx.eventBus().<JsonObject>localConsumer(Constant.EVENTBUS_POLLING, polHandler -> {
                JsonObject result = new JsonObject();

                    JsonObject entries = polHandler.body();

                    entries.stream().forEach((key) -> {
                        var object = entries.getJsonObject(key.getKey());

                        orginalData.put(object.getString(Constant.IPANDGROUP), object.getLong(Constant.TIME));

                        schedulingData.put(object.getString(Constant.IPANDGROUP), object);

                        result.put(Constant.MONITOR_ID, object.getLong("monitorId"));
                    });


                    result.put(Constant.STATUS, Constant.SUCCESS);

                    polHandler.reply(result);

                });

        Bootstrap.vertx.eventBus().<JsonObject>localConsumer(Constant.EVENTBUS_UPDATE_POLLING, updatePolling -> {
            JsonObject result = updatePolling.body();

            for (Map.Entry<String, JsonObject> entries : schedulingData.entrySet()) {
                if (entries.getValue().getString("monitorId").equals(result.getString(Constant.MONITOR_ID)) && entries.getValue().getString(Constant.METRIC_GROUP).equals(result.getString(Constant.METRIC_GROUP)) && entries.getValue().getString(Constant.METRIC_TYPE).equals(result.getString("metricType"))) {
                    entries.getValue().put(Constant.TIME, result.getString("Time"));
                }
            }

            updatePolling.reply("Done");

        });



        Bootstrap.vertx.setPeriodic(10000, polhandling -> {

            for (Map.Entry<String, JsonObject> mapElement : schedulingData.entrySet()) {

                long time = mapElement.getValue().getLong(Constant.TIME);

                if (time <= 0) {
                    Promise<HashMap<String,JsonObject>> promise = Promise.promise();
                    var future = promise.future();

                    schedulingData.put(mapElement.getKey(), mapElement.getValue().put(Constant.TIME,orginalData.get(mapElement.getKey())));

                    schedulingData.put(mapElement.getKey(), mapElement.getValue().put(Constant.METHOD, Constant.EVENTBUS_GET_ALL_SCHEDULINGDATA));

                    Bootstrap.vertx.eventBus().<JsonObject>request(Constant.EVENTBUS_DATABASE, schedulingData.get(mapElement.getKey()), credPolling ->{
                        if (credPolling.succeeded()){
                            JsonObject entries = credPolling.result().body();
                            entries.remove(Constant.METHOD);
                            JsonObject credValue = entries.getJsonObject(Constant.RESULT);
                            schedulingData.put(mapElement.getKey(),mapElement.getValue().mergeIn(credValue));
                            promise.complete(schedulingData);
                        }
                        else {
                            LOGGER.error("No value in Cred");
                            promise.fail(Constant.FAILED);
                        }
                    });

                    future.onComplete(handler ->{
                        if (handler.succeeded()){
                            HashMap<String, JsonObject> data = handler.result();
                            Bootstrap.vertx.eventBus().send(Constant.EVENTBUS_POLLING_ENGINE, data.get(mapElement.getKey()));
                        }
                        else
                        {
                            LOGGER.error(Constant.FAILED);
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
