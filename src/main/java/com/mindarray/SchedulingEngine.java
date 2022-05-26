package com.mindarray;

import com.mindarray.utility.Utility;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;

public class SchedulingEngine extends AbstractVerticle {
    private static final Logger LOGGER = LoggerFactory.getLogger(SchedulingEngine.class);

    @Override
    public void start(Promise<Void> startPromise) {
        LOGGER.debug("POLLER ENGINE DEPLOYED");

        ConcurrentLinkedQueue<JsonObject> queueData = new ConcurrentLinkedQueue<>();

        HashMap<String, Long> orginal = new HashMap<>();

        HashMap<String, Long> schedulingData = new HashMap<>();

        HashMap<String, JsonObject> contextMap = new HashMap<>();


        vertx.eventBus().<JsonObject>request(Constant.EVENTBUS_PRE_POLLING, new JsonObject(), getData -> {
            if (getData.succeeded()) {
                JsonObject entries = getData.result().body();
                entries.stream().forEach((key) -> {
                    var object = entries.getJsonObject(key.getKey());
                    queueData.add(object);
                });
                while (!queueData.isEmpty()) {

                    JsonObject data = queueData.poll();

                    if (data != null) {

                        orginal.put(data.getString(Constant.IPANDGROUP), data.getLong(Constant.TIME));

                        schedulingData.put(data.getString(Constant.IPANDGROUP), data.getLong(Constant.TIME));

                        contextMap.put(data.getString(Constant.IPANDGROUP), data);
                    }

                }
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

                        queueData.add(object);

                        result.put(Constant.MONITOR_ID, object.getLong("monitorId"));
                    });

                    while (!queueData.isEmpty()) {

                        JsonObject data = queueData.poll();

                        if (data != null) {

                            orginal.put(data.getString(Constant.IPANDGROUP), data.getLong(Constant.TIME));

                            schedulingData.put(data.getString(Constant.IPANDGROUP), data.getLong(Constant.TIME));

                            contextMap.put(data.getString(Constant.IPANDGROUP), data);
                        }

                    }


                    result.put(Constant.STATUS, Constant.SUCCESS);

                    polHandler.reply(result);

                } else {

                    polHandler.fail(-1, getData.cause().getMessage());

                }

            });

        });

        vertx.eventBus().<JsonObject>consumer(Constant.EVENTBUS_UPDATE_POLLING, updatePolling -> {
            JsonObject result = updatePolling.body();

            for (JsonObject entries : queueData) {
                if (entries.getString("monitorId").equals(result.getString(Constant.MONITOR_ID)) && entries.getString(Constant.METRIC_GROUP).equals(result.getString(Constant.METRIC_GROUP)) && entries.getString(Constant.METRIC_TYPE).equals(result.getString("metricType"))) {

                    entries.put(Constant.METRIC_GROUP, result.getString(Constant.METRIC_GROUP));
                    entries.put(Constant.METRIC_TYPE, result.getString("metricType"));
                    entries.put(Constant.TIME, result.getString("Time"));
                }
            }

            updatePolling.reply("Done");

        });



        Bootstrap.vertx.setPeriodic(10000, polhandling -> {

            for (Map.Entry<String, Long> mapElement : schedulingData.entrySet()) {

                long time = mapElement.getValue();

                if (time <= 0) {

                    schedulingData.put(mapElement.getKey(), orginal.get(mapElement.getKey()));

                        Bootstrap.vertx.eventBus().<JsonObject>request(Constant.EVENTBUS_POLLING_ENGINE, contextMap.get(mapElement.getKey()), pollingHandler->{
                        if (pollingHandler.succeeded()){
                            LOGGER.info(pollingHandler.result().body().encode());
                        }
                        else {
                            LOGGER.info(pollingHandler.cause().getMessage());
                        }
                        });

                    queueData.add(contextMap.get(mapElement.getKey()));


                } else {
                    time = time - 10000;

                    schedulingData.put(mapElement.getKey(), time);


                }
            }


        });

        startPromise.complete();
    }
}
