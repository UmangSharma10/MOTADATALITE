package com.mindarray.api;

import com.mindarray.Bootstrap;
import com.mindarray.Constant;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.util.ArrayList;

import static com.mindarray.Constant.*;

public class Monitor {

    private static final Logger LOGGER = LoggerFactory.getLogger(Monitor.class);

    public void init(Router monitorRoute) {
        LOGGER.debug("Monitor Class Deployed");

        monitorRoute.put("/monitorMetric/:id").setName("update").handler(this::validate).handler(this::update);

        monitorRoute.delete("/monitor/:id").setName("delete").handler(this::validate).handler(this::delete);

        monitorRoute.get("/monitor/:id/last").setName("get").handler(this::validate).handler(this::getlastInstance);

        monitorRoute.get("/monitor").setName("getAll").handler(this::getAll);

        monitorRoute.get("/monitor/:id/cpuPercent").setName("cpuData").handler(this::validate).handler(this::getCpuPercent);

        monitorRoute.put("/monitor/:id").setName("updateMonitor").handler(this::validate).handler(this::updateMonitor);

    }

    private void validate(RoutingContext routingContext) {
        try {
            var error = new ArrayList<String>();
            JsonObject data = routingContext.getBodyAsJson();

            HttpServerResponse response = routingContext.response();

            if (routingContext.currentRoute().getName().equals("create") || routingContext.currentRoute().getName().equals("update") || routingContext.currentRoute().getName().equals("updateMonitor")) {
                try {
                    if ((data != null)) {
                        data.forEach(key -> {
                            var val = key.getValue();
                            if (val instanceof String) {
                                data.put(key.getKey(), val.toString().trim());
                            }

                        });
                        routingContext.setBody(data.toBuffer());
                    } else {

                        routingContext.response().setStatusCode(400).putHeader(Constant.CONTENT_TYPE, Constant.APPLICATION_JSON).end(new JsonObject().put(Constant.STATUS, Constant.FAILED).put(RESULT, "NO DATA TO FETCH, PLEASE TRY AGAIN LATER.").encode());

                    }
                } catch (Exception exception) {

                    routingContext.response().setStatusCode(500).putHeader(Constant.CONTENT_TYPE, Constant.APPLICATION_JSON).end(new JsonObject().put(Constant.STATUS, Constant.FAILED).put(RESULT, exception.getMessage()).encode());
                }
            }

            switch (routingContext.currentRoute().getName()) {
                case "updateMonitor": {
                    try {
                        LOGGER.debug("updateMonitor");
                        if (routingContext.getBodyAsJson().isEmpty()) {
                            error.add("Invalid Data");
                        }
                        if (routingContext.getBodyAsJson() == null) {
                            error.add("Json is null");
                        }
                        if ((routingContext.getBodyAsJson().containsKey("metric.type"))) {
                            response.setStatusCode(400).putHeader(CONTENT_TYPE, APPLICATION_JSON);
                            response.end(new JsonObject().put(STATUS, FAILED).put(ERROR, "type cannot be updated").encodePrettily());
                            error.add("type cannot be updated");
                            LOGGER.error("type cannot be updated");
                        }
                        if (error.isEmpty()) {
                            if (data != null && routingContext.pathParam(ID) != null) {
                                String id = routingContext.pathParam(ID);
                                Long idL = Long.parseLong(id);
                                data.put(MONITOR_ID, idL);
                                data.put(METHOD, EVENTBUS_CHECK_MONITOR_DATA);
                                Bootstrap.vertx.eventBus().<JsonObject>request(MONITOR_ENDPOINT, data, handler -> {
                                    if (handler.succeeded()) {
                                        JsonObject checkUpdateData = handler.result().body();
                                        if (!checkUpdateData.containsKey(Constant.ERROR)) {
                                            routingContext.next();
                                        }
                                    } else {
                                        response.setStatusCode(400).putHeader(CONTENT_TYPE, APPLICATION_JSON);
                                        response.end(new JsonObject().put(ERROR, handler.cause().getMessage()).put(STATUS, FAILED).encodePrettily());
                                        LOGGER.error(handler.cause().getMessage());
                                    }

                                });
                            } else {
                                LOGGER.error("No data ");
                            }
                        } else {
                            response.setStatusCode(400).putHeader(CONTENT_TYPE, APPLICATION_JSON);
                            response.end(new JsonObject().put(ERROR, error).put(STATUS, FAILED).encodePrettily());
                        }
                    } catch (Exception exception) {
                        routingContext.response().setStatusCode(500).putHeader(CONTENT_TYPE, Constant.APPLICATION_JSON).end(new JsonObject().put(Constant.STATUS, FAILED).put(ERROR, "Json not valid").encode());
                    }
                    break;
                }

                case "get": {
                    LOGGER.debug("getLastInstance");
                    String getId = routingContext.pathParam(ID);
                    Bootstrap.vertx.eventBus().<JsonObject>request(MONITOR_ENDPOINT, new JsonObject().put(METHOD, EVENTBUS_CHECK_PROMONITORDID).put(MONITOR_ID, getId), get -> {
                        if (get.succeeded()) {
                            routingContext.next();
                        } else {
                            String result = get.cause().getMessage();
                            routingContext.response().setStatusCode(400).putHeader(Constant.CONTENT_TYPE, Constant.APPLICATION_JSON).end(result);
                        }

                    });

                    break;
                }
                case "update": {
                    LOGGER.debug("Monitor Metric Update");
                    try {
                        if (routingContext.getBodyAsJson().isEmpty()) {
                            error.add("Invalid Data");
                        }
                        if (routingContext.getBodyAsJson() == null) {
                            error.add("Json is null");
                        }
                        if ((routingContext.getBodyAsJson().containsKey(MONITOR_ID)) || routingContext.getBodyAsJson().getString(MONITOR_ID) == null || routingContext.getBodyAsJson().getString(MONITOR_ID).isBlank()) {
                            response.setStatusCode(400).putHeader(CONTENT_TYPE, APPLICATION_JSON);
                            response.end(new JsonObject().put(STATUS, FAILED).put(ERROR, "Id can't be provided").encodePrettily());
                            LOGGER.error("id cant be provided");
                        }
                        if (!(routingContext.getBodyAsJson().containsKey("Time")) || routingContext.getBodyAsJson().getString("Time") == null || routingContext.getBodyAsJson().getString("Time").isBlank()) {
                            response.setStatusCode(400).putHeader(CONTENT_TYPE, APPLICATION_JSON);
                            response.end(new JsonObject().put(STATUS, FAILED).put(ERROR, "Time is null, blank or not provided").encodePrettily());
                            LOGGER.error("Time is null , blank or not provided");
                        }
                        if (!(routingContext.getBodyAsJson().containsKey("metricType")) || routingContext.getBodyAsJson().getString("metricType") == null || routingContext.getBodyAsJson().getString("metricType").isBlank()) {
                            response.setStatusCode(400).putHeader(CONTENT_TYPE, APPLICATION_JSON);
                            response.end(new JsonObject().put(STATUS, FAILED).put(ERROR, "type is null, blank or not provided").encodePrettily());
                            LOGGER.error("type is null , blank or not provided");
                        }
                        if (!(routingContext.getBodyAsJson().containsKey(METRIC_GROUP)) || routingContext.getBodyAsJson().getString(METRIC_GROUP) == null || routingContext.getBodyAsJson().getString(METRIC_GROUP).isBlank()) {
                            response.setStatusCode(400).putHeader(CONTENT_TYPE, APPLICATION_JSON);
                            response.end(new JsonObject().put(STATUS, FAILED).put(ERROR, "group is null, blank or not provided").encodePrettily());
                            LOGGER.error("group is null , blank or not provided");
                        }
                        if (error.isEmpty()) {
                            if (data != null && routingContext.pathParam(ID) != null) {
                                String id = routingContext.pathParam(ID);
                                Long idL = Long.parseLong(id);
                                data.put(MONITOR_ID, idL);
                                data.put(METHOD, EVENTBUS_CHECK_MONITORMETRIC);
                                Bootstrap.vertx.eventBus().<JsonObject>request(EVENTBUS_DATABASE, data, handler -> {
                                    if (handler.succeeded()) {
                                        JsonObject checkUpdateData = handler.result().body();
                                        if (!checkUpdateData.containsKey(Constant.ERROR)) {
                                            routingContext.next();
                                        }
                                    } else {
                                        response.setStatusCode(400).putHeader(CONTENT_TYPE, APPLICATION_JSON);
                                        response.end(new JsonObject().put(ERROR, handler.cause().getMessage()).put(STATUS, FAILED).encodePrettily());
                                        LOGGER.error(handler.cause().getMessage());
                                    }

                                });
                            } else {
                                LOGGER.error("No data");
                            }
                        } else {
                            response.setStatusCode(400).putHeader(CONTENT_TYPE, APPLICATION_JSON);
                            response.end(new JsonObject().put(ERROR, error).put(STATUS, FAILED).encodePrettily());
                        }
                    } catch (Exception exception) {
                        routingContext.response().setStatusCode(500).putHeader(CONTENT_TYPE, Constant.APPLICATION_JSON).end(new JsonObject().put(Constant.STATUS, FAILED).put(ERROR, "Json not valid").encode());
                    }
                    break;
                }
                case "delete": {
                    LOGGER.debug("delete Route");
                    if (routingContext.pathParam(ID) != null) {
                        String id = routingContext.pathParam(ID);
                        Bootstrap.vertx.eventBus().<JsonObject>request(MONITOR_ENDPOINT, new JsonObject().put(METHOD, EVENTBUS_CHECK_PROMONITORDID).put(MONITOR_ID, id), deleteid -> {
                            if (deleteid.succeeded()) {
                                routingContext.next();
                            } else {
                                response.setStatusCode(400).putHeader(CONTENT_TYPE, APPLICATION_JSON);
                                response.end(new JsonObject().put(ERROR, deleteid.cause().getMessage()).put(STATUS, FAILED).encodePrettily());
                                LOGGER.error(deleteid.cause().getMessage());
                            }
                        });
                    } else {
                        response.setStatusCode(400).putHeader(CONTENT_TYPE, APPLICATION_JSON);
                        response.end(new JsonObject().put(ERROR, "id is null").put(STATUS, FAILED).encodePrettily());
                        LOGGER.error("id is null");
                    }

                    break;
                }
                case "cpuData": {
                    LOGGER.debug("getById");
                    String cpuDataID = routingContext.pathParam(ID);
                    Bootstrap.vertx.eventBus().<JsonObject>request(MONITOR_ENDPOINT, new JsonObject().put(MONITOR_ID, cpuDataID).put(METHOD, EVENTBUS_CHECK_PROMONITORDID), get -> {
                        if (get.succeeded()) {
                            routingContext.next();
                        } else {
                            String result = get.cause().getMessage();
                            routingContext.response().setStatusCode(400).putHeader(Constant.CONTENT_TYPE, Constant.APPLICATION_JSON).end(result);
                        }

                    });
                    break;
                }

            }
        } catch (Exception exception) {
            routingContext.response().setStatusCode(500).putHeader(CONTENT_TYPE, Constant.APPLICATION_JSON).end(new JsonObject().put(Constant.STATUS, Constant.FAILED).put(ERROR, "JSON NOT VALID").encode());
        }
    }


    private void updateMonitor(RoutingContext routingContext) {
        String id = routingContext.pathParam(ID);
        Long idL = Long.parseLong(id);
        JsonObject data = routingContext.getBodyAsJson();
        if (data != null) {
            data.put(MONITOR_ID, idL);
            data.put(METHOD, EVENTBUS_UPDATE_MONITOR);
            Bootstrap.vertx.eventBus().<JsonObject>request(MONITOR_ENDPOINT, data, handler -> {
                if (handler.succeeded()) {
                    JsonObject checkUpdateData = handler.result().body();
                    routingContext.response().setStatusCode(200).putHeader(CONTENT_TYPE, APPLICATION_JSON).end(checkUpdateData.encode());
                } else {
                    String result = handler.cause().getMessage();
                    routingContext.response().setStatusCode(400).putHeader(CONTENT_TYPE, APPLICATION_JSON);
                    routingContext.response().end(new JsonObject().put(ERROR, result).encode());
                    LOGGER.error(handler.cause().getMessage());
                }

            });
        } else {
            routingContext.response().setStatusCode(400).putHeader(CONTENT_TYPE, APPLICATION_JSON);
            routingContext.response().end(new JsonObject().put(ERROR, "No data").encode());
        }
    }

    private void getAll(RoutingContext routingContext) {
        try {
            int sec = LocalDateTime.now().getSecond();
            Bootstrap.vertx.eventBus().<JsonObject>request(MONITOR_ENDPOINT, new JsonObject().put(METHOD, EVENTBUS_GET_ALL_MONITOR), getAllHandler -> {
                if (getAllHandler.succeeded()) {
                    JsonObject getData = getAllHandler.result().body();
                    LOGGER.debug("Response {}", getAllHandler.result().body());
                    LOGGER.debug("After {}", sec - LocalDateTime.now().getSecond());
                    routingContext.response().setStatusCode(200).putHeader(CONTENT_TYPE, Constant.APPLICATION_JSON).end(getData.encode());
                } else {
                    String result = getAllHandler.cause().getMessage();
                    routingContext.response().setStatusCode(400).putHeader(CONTENT_TYPE, Constant.APPLICATION_JSON).end(result);
                }
            });
        } catch (Exception exception) {
            routingContext.response().setStatusCode(500).putHeader(CONTENT_TYPE, Constant.APPLICATION_JSON).end(new JsonObject().put(Constant.STATUS, Constant.FAILED).encode());
        }

    }

    private void getlastInstance(RoutingContext routingContext) {
        try {
            String getId = routingContext.pathParam(ID);
            Bootstrap.vertx.eventBus().<JsonObject>request(MONITOR_ENDPOINT, new JsonObject().put(METHOD, EVENTBUS_GET_MONITOR_BY_ID).put(MONITOR_ID, getId), getLastInstanceHandler -> {
                if (getLastInstanceHandler.succeeded()) {
                    JsonObject getData = getLastInstanceHandler.result().body();
                    LOGGER.debug("Response {} ", getLastInstanceHandler.result().body());
                    routingContext.response().setStatusCode(200).putHeader(CONTENT_TYPE, Constant.APPLICATION_JSON).end(getData.encode());
                } else {
                    String result = getLastInstanceHandler.cause().getMessage();
                    routingContext.response().setStatusCode(400).putHeader(CONTENT_TYPE, Constant.APPLICATION_JSON).end(result);
                }
            });
        } catch (Exception exception) {
            LOGGER.error(exception.getMessage());
            routingContext.response().setStatusCode(500).putHeader(CONTENT_TYPE, Constant.APPLICATION_JSON).end(exception.getMessage());
        }
    }

    private void delete(RoutingContext routingContext) {
        try {
            String id = routingContext.pathParam("id");

            Bootstrap.vertx.eventBus().<JsonObject>request(MONITOR_ENDPOINT, new JsonObject().put(METHOD, EVENTBUS_DELETE_PROVISION).put(MONITOR_ID, id), deletebyID -> {
                if (deletebyID.succeeded()) {
                    JsonObject deleteResult = deletebyID.result().body();
                    LOGGER.debug("Response {} ", deletebyID.result().body());
                    routingContext.response().setStatusCode(200).putHeader(CONTENT_TYPE, Constant.APPLICATION_JSON).end(deleteResult.encode());
                } else {
                    String result = deletebyID.cause().getMessage();
                    routingContext.response().setStatusCode(400).putHeader(CONTENT_TYPE, Constant.APPLICATION_JSON).end(result);
                }
            });

        } catch (Exception exception) {
            routingContext.response().setStatusCode(500).putHeader(CONTENT_TYPE, Constant.APPLICATION_JSON).end(new JsonObject().put(Constant.STATUS, Constant.FAILED).encode());
        }
    }

    private void update(RoutingContext routingContext) {
        try {
            JsonObject createData = routingContext.getBodyAsJson();
            String id = routingContext.pathParam(ID);
            Long idL = Long.parseLong(id);
            createData.put(MONITOR_ID, idL);
            Bootstrap.vertx.eventBus().<JsonObject>request(EVENTBUS_UPDATE_METRIC, createData, updateHandler -> {
                if (updateHandler.succeeded()) {
                    JsonObject dbData = updateHandler.result().body();
                    LOGGER.debug("Response {} ", updateHandler.result().body());
                    routingContext.response().setStatusCode(200).putHeader(CONTENT_TYPE, Constant.APPLICATION_JSON).end(dbData.encode());
                } else {
                    routingContext.response().setStatusCode(400).putHeader(CONTENT_TYPE, APPLICATION_JSON);
                    routingContext.response().end(new JsonObject().put(STATUS, FAILED).put(ERROR, updateHandler.cause().getMessage()).encodePrettily());
                    LOGGER.error(updateHandler.cause().getMessage());
                }
            });
        } catch (Exception exception) {
            routingContext.response().setStatusCode(500).putHeader("content-type", Constant.APPLICATION_JSON).end(new JsonObject().put(Constant.STATUS, Constant.FAILED).encode());
        }

    }

    private void getCpuPercent(RoutingContext routingContext) {
        try {
            String getId = routingContext.pathParam(ID);
            Bootstrap.vertx.eventBus().<JsonObject>request(MONITOR_ENDPOINT, new JsonObject().put(METHOD, EVENTBUS_GET_CPUPERCENT).put(MONITOR_ID, getId), getLastInstanceHandler -> {
                if (getLastInstanceHandler.succeeded()) {
                    JsonObject getData = getLastInstanceHandler.result().body();
                    LOGGER.debug("Response {} ", getLastInstanceHandler.result().body());
                    routingContext.response().setStatusCode(200).putHeader(CONTENT_TYPE, Constant.APPLICATION_JSON).end(getData.encode());
                } else {
                    String result = getLastInstanceHandler.cause().getMessage();
                    routingContext.response().setStatusCode(400).putHeader(CONTENT_TYPE, Constant.APPLICATION_JSON).end(result);
                }
            });
        } catch (Exception exception) {
            LOGGER.error(exception.getMessage());
            routingContext.response().setStatusCode(500).putHeader(CONTENT_TYPE, Constant.APPLICATION_JSON).end(exception.getMessage());
        }
    }


}
