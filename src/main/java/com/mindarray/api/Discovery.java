package com.mindarray.api;

import com.mindarray.Bootstrap;
import com.mindarray.Constant;
import com.mindarray.utility.Utility;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;

import static com.mindarray.Constant.*;

public class Discovery {
    private static final Logger LOGGER = LoggerFactory.getLogger(Discovery.class);

    Utility utility = new Utility();
    public void init(Router discoveryRoute) {

        LOGGER.debug("Discovery Route deployed");

        discoveryRoute.post(DISCOVERY_ENDPOINT).setName("create").handler(this::validate).handler(this::create);

        discoveryRoute.get(DISCOVERY_ENDPOINT + "/:id").setName("getId").handler(this::validate).handler(this::getById);

        discoveryRoute.get(DISCOVERY_ENDPOINT).setName("getAll").handler(this::validate).handler(this::getAll);

        discoveryRoute.delete(DISCOVERY_ENDPOINT + "/:id").setName("delete").handler(this::validate).handler(this::delete);

        discoveryRoute.put(DISCOVERY_ENDPOINT).setName("update").handler(this::validate).handler(this::update);

        discoveryRoute.post(PROVISION_ENDPOINT + "/:id").setName("provision").handler(this::createProvision);

        discoveryRoute.post(DISCOVERY_ENDPOINT + "/:id/run").setName("run").handler(this::runDiscovery);
    }


    private void validate(RoutingContext routingContext) {
        try {

            HttpServerResponse response = routingContext.response();

            var error = new ArrayList<String>();

            JsonObject data = routingContext.getBodyAsJson();


            if (routingContext.currentRoute().getName().equals("create") || routingContext.currentRoute().getName().equals("update")) {

                try {
                    if (data != null) {

                        data.forEach(key -> {
                            var val = key.getValue();
                            if (val instanceof String) {
                                data.put(key.getKey(), val.toString().trim());
                            }

                        });
                        routingContext.setBody(data.toBuffer());
                    } else {

                        routingContext.response().setStatusCode(400).putHeader(CONTENT_TYPE, Constant.APPLICATION_JSON).end(new JsonObject().put(Constant.STATUS, Constant.FAILED).encode());

                    }
                } catch (Exception exception) {

                    routingContext.response().setStatusCode(400).putHeader(CONTENT_TYPE, Constant.APPLICATION_JSON).end(new JsonObject().put(Constant.STATUS, Constant.FAILED).encode());
                }
            }

            switch (routingContext.currentRoute().getName()) {
                case CREATE:
                    LOGGER.debug("Create Route");
                    if (!(routingContext.getBodyAsJson().containsKey(DIS_NAME)) || routingContext.getBodyAsJson().getString(DIS_NAME) == null || routingContext.getBodyAsJson().getString(DIS_NAME).isBlank()) {
                        error.add("Discovery name is null or blank");
                    }
                    if (!(routingContext.getBodyAsJson().containsKey(CRED_PROFILE)) || routingContext.getBodyAsJson().getString(CRED_PROFILE) == null || routingContext.getBodyAsJson().getString(CRED_PROFILE).isBlank()) {
                        error.add("Credential Profile is null or blank");
                    }
                    if (!(routingContext.getBodyAsJson().containsKey(IP_ADDRESS)) || routingContext.getBodyAsJson().getString(IP_ADDRESS) == null || routingContext.getBodyAsJson().getString(IP_ADDRESS).isBlank()) {
                        error.add("IP is null is blank");
                    }
                    if(Boolean.FALSE.equals(utility.isValidIp(routingContext.getBodyAsJson().getString(IP_ADDRESS)))){
                        error.add("IP address is not valid");
                    }
                    if (!(routingContext.getBodyAsJson().containsKey(METRIC_TYPE)) || routingContext.getBodyAsJson().getString(METRIC_TYPE) == null || routingContext.getBodyAsJson().getString(METRIC_TYPE).isBlank()) {
                        error.add("metric.type is null or blank ");
                    }
                    if (!(routingContext.getBodyAsJson().containsKey(PORT)) || routingContext.getBodyAsJson().getInteger(PORT) == null || routingContext.getBodyAsJson().isEmpty()) {
                        error.add("Port not defined for discovery or null or blank");
                    }
                    if (Boolean.FALSE.equals(utility.isValidPort(String.valueOf(routingContext.getBodyAsJson().getInteger(PORT))))){
                        error.add("port out of scope");
                    }
                    if (!(routingContext.getBodyAsJson().containsKey(CRED_PROFILE)) || routingContext.getBodyAsJson().getString(CRED_PROFILE) == null || routingContext.getBodyAsJson().getString(CRED_PROFILE).isBlank()) {
                        error.add("Credential Profile not defined for discovery or null");
                    }

                    if (error.isEmpty()) {
                        if (data != null) {
                            data.put(METHOD, EVENTBUS_CHECK_DISNAME);
                            Bootstrap.vertx.eventBus().<JsonObject>request(EVENTBUS_DATABASE, data, handler -> {
                                if (handler.succeeded()) {
                                    JsonObject checkNameData = handler.result().body();
                                    if (!checkNameData.containsKey(Constant.ERROR)) {
                                        routingContext.next();
                                    }
                                } else {
                                    response.setStatusCode(400).putHeader(CONTENT_TYPE, APPLICATION_JSON);
                                    response.end(new JsonObject().put(ERROR, handler.cause().getMessage()).put(STATUS, FAILED).encodePrettily());
                                    LOGGER.error(handler.cause().getMessage());
                                }
                            });
                        } else {
                            LOGGER.error("Data is not there");
                        }
                    } else {
                        response.setStatusCode(400).putHeader(CONTENT_TYPE, APPLICATION_JSON);
                        response.end(new JsonObject().put(ERROR, error).put(STATUS, FAILED).encodePrettily());

                    }
                    break;
                case DELETE:
                    LOGGER.debug("delete Route");
                    if (routingContext.pathParam("id") != null) {
                        String id = routingContext.pathParam("id");
                        Bootstrap.vertx.eventBus().<JsonObject>request(EVENTBUS_DATABASE, new JsonObject().put(METHOD, EVENTBUS_CHECKID_DISCOVERY).put(DIS_ID, id), deleteid -> {
                            if (deleteid.succeeded()) {
                                JsonObject deleteIdData = deleteid.result().body();
                                if (!deleteIdData.containsKey(Constant.ERROR)) {
                                    routingContext.next();
                                }
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

                case UPDATE:
                    LOGGER.debug("Update Route");
                    if (!(routingContext.getBodyAsJson().containsKey(DIS_ID)) || routingContext.getBodyAsJson().getString(DIS_ID) == null || routingContext.getBodyAsJson().getString(DIS_ID).isBlank()) {
                        response.setStatusCode(400).putHeader(CONTENT_TYPE, APPLICATION_JSON);
                        response.end(new JsonObject().put(STATUS, FAILED).put(ERROR, "Id is null").encodePrettily());
                        LOGGER.error("id is null");
                    }
                    if (routingContext.getBodyAsJson().containsKey(METRIC_TYPE)){
                        response.setStatusCode(400).putHeader(CONTENT_TYPE, APPLICATION_JSON);
                        response.end(new JsonObject().put(STATUS, FAILED).put(ERROR, "Cannot update Metric Type").encodePrettily());
                        LOGGER.error("Cannot update Metric Type");
                    } else {
                        if (data != null) {
                            data.put(METHOD, EVENTBUS_CHECKID_DISCOVERY);
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
                    }
                    break;
                case GETID:
                    LOGGER.debug("Get Routing");
                    if (routingContext.pathParam(ID) == null) {
                        response.setStatusCode(400).putHeader(CONTENT_TYPE, APPLICATION_JSON);
                        response.end(new JsonObject().put(ERROR, "id is null").put(STATUS, FAILED).encodePrettily());
                        LOGGER.error("id is null");
                    } else {
                        String getId = routingContext.pathParam(ID);
                        Bootstrap.vertx.eventBus().<JsonObject>request(EVENTBUS_DATABASE, new JsonObject().put(METHOD, EVENTBUS_CHECKID_DISCOVERY).put(DIS_ID, getId), get -> {
                            if (get.succeeded()) {
                                JsonObject getDisData = get.result().body();
                                if (!getDisData.containsKey(Constant.ERROR)) {
                                    routingContext.next();
                                }
                            } else {
                                String result = get.cause().getMessage();
                                routingContext.response().setStatusCode(400).putHeader(CONTENT_TYPE, Constant.APPLICATION_JSON).end(result);
                            }

                        });
                    }
                    break;
                case GETALL:
                    LOGGER.debug("Get ALL");
                    routingContext.next();

            }
        } catch (Exception exception) {
            routingContext.response().setStatusCode(400).putHeader(CONTENT_TYPE, Constant.APPLICATION_JSON).end(new JsonObject().put(Constant.STATUS, Constant.FAILED).encode());
        }
    }

    private void update(RoutingContext routingContext) {
        try {

            JsonObject createData = routingContext.getBodyAsJson();
            createData.put(METHOD, EVENTBUS_UPDATE_DIS);
            Bootstrap.vertx.eventBus().<JsonObject>request(EVENTBUS_DATABASE, createData, updateHandler -> {
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
            routingContext.response().setStatusCode(400).putHeader(CONTENT_TYPE, Constant.APPLICATION_JSON).end(new JsonObject().put(Constant.STATUS, Constant.FAILED).encode());
        }
    }

    private void delete(RoutingContext routingContext) {
        try {
            String id = routingContext.pathParam(ID);

            Bootstrap.vertx.eventBus().<JsonObject>request(EVENTBUS_DATABASE, new JsonObject().put(METHOD, EVENTBUS_DELETEDIS).put(DIS_ID, id), deletebyID -> {
                if (deletebyID.succeeded()) {
                    JsonObject deleteResult = deletebyID.result().body();
                    LOGGER.debug("Response {} ", deletebyID.result().body());
                    routingContext.response().setStatusCode(200).putHeader("content-type", Constant.APPLICATION_JSON).end(deleteResult.encode());
                } else {
                    String result = deletebyID.cause().getMessage();
                    routingContext.response().setStatusCode(200).putHeader(CONTENT_TYPE, Constant.APPLICATION_JSON).end(result);
                }
            });

        } catch (Exception exception) {
            routingContext.response().setStatusCode(400).putHeader("content-type", Constant.APPLICATION_JSON).end(new JsonObject().put(Constant.STATUS, Constant.FAILED).encode());
        }
    }

    private void getById(RoutingContext routingContext) {
        try {
            String getId = routingContext.pathParam(ID);
            Bootstrap.vertx.eventBus().<JsonObject>request(EVENTBUS_DATABASE, new JsonObject().put(METHOD, EVENTBUS_GETDISCOVERY).put(DIS_ID, getId), getbyIdHandler -> {
                if (getbyIdHandler.succeeded()) {
                    JsonObject getData = getbyIdHandler.result().body();
                    LOGGER.debug("Response {} ", getbyIdHandler.result().body());
                    routingContext.response().setStatusCode(200).putHeader(CONTENT_TYPE, Constant.APPLICATION_JSON).end(getData.encode());
                } else {
                    String result = getbyIdHandler.cause().getMessage();
                    routingContext.response().setStatusCode(200).putHeader(CONTENT_TYPE, Constant.APPLICATION_JSON).end(result);
                }
            });
        } catch (Exception exception) {
            routingContext.response().setStatusCode(400).putHeader(CONTENT_TYPE, Constant.APPLICATION_JSON).end(new JsonObject().put(Constant.STATUS, Constant.FAILED).encode());
        }

    }

    private void getAll(RoutingContext routingContext) {
        try {
            Bootstrap.vertx.eventBus().<JsonObject>request(EVENTBUS_DATABASE, new JsonObject().put(METHOD, EVENTBUS_GETALLDIS), getAllHandler -> {
                if (getAllHandler.succeeded()) {
                    JsonObject getData = getAllHandler.result().body();
                    LOGGER.debug("Response {}", getAllHandler.result().body());
                    routingContext.response().setStatusCode(200).putHeader(CONTENT_TYPE, Constant.APPLICATION_JSON).end(getData.encode());
                } else {
                    String result = getAllHandler.cause().getMessage();
                    routingContext.response().setStatusCode(400).putHeader(CONTENT_TYPE, Constant.APPLICATION_JSON).end(result);
                }
            });
        } catch (Exception exception) {
            routingContext.response().setStatusCode(400).putHeader(CONTENT_TYPE, Constant.APPLICATION_JSON).end(new JsonObject().put(Constant.STATUS, Constant.FAILED).encode());
        }
    }

    private void create(RoutingContext routingContext) {
        try {
            JsonObject createData = routingContext.getBodyAsJson();
            createData.put(METHOD, EVENTBUS_INSERTDISCOVERY);
            Bootstrap.vertx.eventBus().<JsonObject>request(EVENTBUS_DATABASE, createData, createHandler -> {
                if (createHandler.succeeded()) {
                    JsonObject dbData = createHandler.result().body();
                    LOGGER.debug("Response {} ", createHandler.result().body());
                    routingContext.response().setStatusCode(200).putHeader(CONTENT_TYPE, Constant.APPLICATION_JSON).end(dbData.encode());
                } else {
                    String result = createHandler.cause().getMessage();
                    routingContext.response().setStatusCode(400).putHeader(CONTENT_TYPE, Constant.APPLICATION_JSON).end(result);
                }
            });
        } catch (Exception exception) {
            routingContext.response().setStatusCode(400).putHeader("content-type", Constant.APPLICATION_JSON).end(new JsonObject().put(Constant.STATUS, Constant.FAILED).encode());
        }
    }

    private void createProvision(RoutingContext routingContext) {
        try {
            String id = routingContext.pathParam(ID);
            Bootstrap.vertx.eventBus().<JsonObject>request(EVENTBUS_PROVISION, new JsonObject().put(DIS_ID, id), provisionByID -> {

                if (provisionByID.succeeded()) {
                    JsonObject runResult = provisionByID.result().body();
                    LOGGER.debug("Response {} ", provisionByID.result().body());
                    routingContext.response().setStatusCode(200).putHeader("content-type", Constant.APPLICATION_JSON).end(runResult.encode());
                } else {

                    String runResult = provisionByID.cause().getMessage();
                    LOGGER.debug("Response {} ", runResult);
                    routingContext.response().setStatusCode(400).putHeader("content-type", Constant.APPLICATION_JSON).end(runResult);
                }


            });

        } catch (Exception exception) {
            routingContext.response().setStatusCode(400).putHeader("content-type", Constant.APPLICATION_JSON).end(new JsonObject().put(Constant.STATUS, Constant.FAILED).encode());
        }

    }

    private void runDiscovery(RoutingContext routingContext) {
        try {
            String id = routingContext.pathParam(ID);
            Bootstrap.vertx.eventBus().<JsonObject>request(EVENTBUS_DATABASE, new JsonObject().put(DIS_ID, id).put(METHOD, EVENTBUS_RUN_DISCOVERY), runDiscoverybyID -> {

                if (runDiscoverybyID.succeeded()) {
                    JsonObject runResult = runDiscoverybyID.result().body();
                    LOGGER.debug("Response {} ", runDiscoverybyID.result().body());
                    routingContext.response().setStatusCode(200).putHeader("content-type", Constant.APPLICATION_JSON).end(runResult.encode());
                } else {
                    String runResult = runDiscoverybyID.cause().getMessage();
                    LOGGER.debug("Response {} ", runResult);
                    routingContext.response().setStatusCode(400).putHeader(CONTENT_TYPE, Constant.APPLICATION_JSON).end(runResult);
                }


            });

        } catch (Exception exception) {
            routingContext.response().setStatusCode(400).putHeader("content-type", Constant.APPLICATION_JSON).end(new JsonObject().put(Constant.STATUS, Constant.FAILED).encode());
        }


    }

}
