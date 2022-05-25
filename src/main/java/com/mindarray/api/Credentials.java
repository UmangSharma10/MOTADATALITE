package com.mindarray.api;

import com.mindarray.Bootstrap;
import com.mindarray.Constant;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;

import static com.mindarray.Constant.*;


public class Credentials {

    private static final Logger LOGGER = LoggerFactory.getLogger(Credentials.class);

    public void init(Router credentialRoute) {

        LOGGER.debug("Cred Route Deployed");

        credentialRoute.post(CREDENTIAL_ENDPOINT).setName("create").handler(this::validate).handler(this::create);

        credentialRoute.get(CREDENTIAL_ENDPOINT + "/:id").setName("getId").handler(this::validate).handler(this::getByID);

        credentialRoute.get(CREDENTIAL_ENDPOINT).setName("getAll").handler(this::validate).handler(this::getAll);

        credentialRoute.delete(CREDENTIAL_ENDPOINT + "/:id").setName("delete").handler(this::validate).handler(this::delete);

        credentialRoute.put(CREDENTIAL_ENDPOINT).setName("update").handler(this::validate).handler(this::update);
    }

    private void validate(RoutingContext routingContext) {
        try {
            var error = new ArrayList<String>();

            HttpServerResponse response = routingContext.response();

            JsonObject data = routingContext.getBodyAsJson();

            if (routingContext.currentRoute().getName().equals("create") || routingContext.currentRoute().getName().equals("update")) {

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

                        routingContext.response().setStatusCode(400).putHeader(CONTENT_TYPE, Constant.APPLICATION_JSON).end(new JsonObject().put(Constant.STATUS, Constant.FAILED).encode());

                    }
                } catch (Exception exception) {

                    routingContext.response().setStatusCode(400).putHeader(CONTENT_TYPE, Constant.APPLICATION_JSON).end(new JsonObject().put(Constant.STATUS, Constant.FAILED).encode());
                }
            }

            switch (routingContext.currentRoute().getName()) {
                case CREATE:
                    LOGGER.debug("Create Route");

                    if (routingContext.getBodyAsJson().containsKey(PROTOCOL)) {

                        if (routingContext.getBodyAsJson().getString(PROTOCOL).equals(SSH) || routingContext.getBodyAsJson().getString(PROTOCOL).equals(WINRM)) {

                            if (!(routingContext.getBodyAsJson().containsKey(USER)) || routingContext.getBodyAsJson().getString(USER) == null || routingContext.getBodyAsJson().getString(USER).isBlank()) {

                                error.add("User not provided, null or blank");

                            }

                            if (!(routingContext.getBodyAsJson().containsKey(PASSWORD)) || routingContext.getBodyAsJson().getString(PASSWORD) == null || routingContext.getBodyAsJson().getString(PASSWORD).isBlank()) {

                                error.add("Password not provided, null or blank");

                            }

                            if (routingContext.getBodyAsJson().containsKey(VERSION)) {

                                error.add("Version key field in Protocol " + routingContext.getBodyAsJson().getString(PROTOCOL));

                            }

                            if (routingContext.getBodyAsJson().containsKey(COMMUNITY)) {

                                error.add("Community Key field in Protocol " + routingContext.getBodyAsJson().getString(PROTOCOL));

                            }

                        } else if (routingContext.getBodyAsJson().getString(PROTOCOL).equals("snmp")) {

                            if (!(routingContext.getBodyAsJson().containsKey(COMMUNITY)) || routingContext.getBodyAsJson().getString(COMMUNITY) == null || routingContext.getBodyAsJson().getString(COMMUNITY).isBlank()) {

                                error.add("Community not provided , null or blank");

                            }

                            if (!(routingContext.getBodyAsJson().containsKey(VERSION)) || routingContext.getBodyAsJson().getString(VERSION) == null || routingContext.getBodyAsJson().getString(VERSION).isBlank()) {

                                error.add("Version not provided, null or blank");

                            }

                            if (routingContext.getBodyAsJson().containsKey(USER)) {

                                error.add("Username Key field in Protocol" + routingContext.getBodyAsJson().getString(PROTOCOL));

                            }

                            if (routingContext.getBodyAsJson().containsKey(PASSWORD)) {

                                error.add("Password Key field in Protocol" + routingContext.getBodyAsJson().getString(PROTOCOL));

                            }

                        } else {

                            error.add("Wrong protocol selected");

                        }

                    }
                    if (!(routingContext.getBodyAsJson().containsKey(CRED_NAME)) || routingContext.getBodyAsJson().getString(CRED_NAME) == null || routingContext.getBodyAsJson().getString(CRED_NAME).isBlank()) {
                        error.add("credential name not provided, null or blank");
                    }
                    if (error.isEmpty()) {
                        if (data != null) {
                            data.put(METHOD, EVENTBUS_CHECK_CREDNAME);
                            Bootstrap.vertx.eventBus().<JsonObject>request(EVENTBUS_DATABASE, data, handler -> {
                                if (handler.succeeded()) {
                                    JsonObject checkNameData = handler.result().body();
                                    if (!checkNameData.containsKey(Constant.ERROR)) {
                                        routingContext.next();
                                    }
                                } else {
                                    String result = handler.cause().getMessage();
                                    routingContext.response().setStatusCode(400).putHeader(CONTENT_TYPE, Constant.APPLICATION_JSON).end(result);
                                }

                            });
                        } else {
                            LOGGER.error("Data is not there");
                        }
                    } else {
                        response.setStatusCode(400).putHeader(CONTENT_TYPE, APPLICATION_JSON);
                        response.end(new JsonObject().put(ERROR, error).put(STATUS, FAILED).encodePrettily());
                        LOGGER.error("Error occurred{}", error);
                    }
                    break;
                case DELETE:
                    LOGGER.debug("delete Route");
                    String id = routingContext.pathParam(ID);
                    Bootstrap.vertx.eventBus().<JsonObject>request(EVENTBUS_DATABASE, new JsonObject().put(CRED_ID, id).put(METHOD, EVENTBUS_CHECKID_CRED), deleteid -> {
                        if (deleteid.succeeded()) {
                            JsonObject deleteIdData = deleteid.result().body();
                            if (!deleteIdData.containsKey(Constant.ERROR)) {
                                routingContext.next();
                            }
                        } else {
                            String result = deleteid.cause().getMessage();
                            routingContext.response().setStatusCode(400).putHeader(CONTENT_TYPE, Constant.APPLICATION_JSON).end(result);
                        }
                    });
                    break;

                case UPDATE:
                    LOGGER.debug("Update Route");
                    if (!(routingContext.getBodyAsJson().containsKey(CRED_ID)) || routingContext.getBodyAsJson().getString(CRED_ID) == null || routingContext.getBodyAsJson().getString(CRED_ID).isBlank()) {
                        response.setStatusCode(400).putHeader(CONTENT_TYPE, APPLICATION_JSON);
                        response.end(new JsonObject().put(ERROR, "id is null or blank or not provided").put(STATUS, FAILED).encodePrettily());
                        LOGGER.error("id is null or blank or not provided");
                    }
                    if (!(routingContext.getBodyAsJson().containsKey(PROTOCOL)) || routingContext.getBodyAsJson().getString(PROTOCOL) == null || routingContext.getBodyAsJson().getString(PROTOCOL).isBlank()) {
                        response.setStatusCode(400).putHeader(CONTENT_TYPE, APPLICATION_JSON);
                        response.end(new JsonObject().put(ERROR, "protocol is null or blank or not provided").put(STATUS, FAILED).encodePrettily());
                        LOGGER.error("protocol is null or blank or not provided");
                    } else {
                        if (data != null) {
                            data.put(METHOD, EVENTBUS_CHECKID_CRED);
                            Bootstrap.vertx.eventBus().<JsonObject>request(EVENTBUS_DATABASE, data, handler -> {
                                if (handler.succeeded()) {
                                    JsonObject checkUpdateData = handler.result().body();
                                    if (!checkUpdateData.containsKey(Constant.ERROR)) {
                                        routingContext.next();
                                    }
                                } else {
                                    String result = handler.cause().getMessage();
                                    routingContext.response().setStatusCode(400).putHeader(CONTENT_TYPE, Constant.APPLICATION_JSON).end(result);
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
                    } else {
                        String getId = routingContext.pathParam(ID);
                        Bootstrap.vertx.eventBus().<JsonObject>request(EVENTBUS_DATABASE, new JsonObject().put(METHOD, EVENTBUS_CHECKID_CRED).put(CRED_ID, getId), get -> {
                            if (get.succeeded()) {
                                routingContext.next();
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


    private void getAll(RoutingContext routingContext) {
        try {
            Bootstrap.vertx.eventBus().<JsonObject>request(EVENTBUS_DATABASE, new JsonObject().put(METHOD, EVENTBUS_GETALLCRED), getAllHandler -> {
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

    private void update(RoutingContext routingContext) {
        try {
            JsonObject createData = routingContext.getBodyAsJson();
            createData.put(METHOD, EVENTBUS_UPDATE_CRED);
            Bootstrap.vertx.eventBus().<JsonObject>request(EVENTBUS_DATABASE, createData, updateHandler -> {

                if (updateHandler.succeeded()) {
                    JsonObject dbData = updateHandler.result().body();
                    if (!dbData.containsKey(ERROR)) {
                        LOGGER.debug("Response {} ", updateHandler.result().body());
                        routingContext.response().setStatusCode(200).putHeader(CONTENT_TYPE, Constant.APPLICATION_JSON).end(dbData.encode());
                    }
                } else {
                    String result = updateHandler.cause().getMessage();
                    routingContext.response().setStatusCode(400).putHeader(CONTENT_TYPE, Constant.APPLICATION_JSON).end(result);
                }
            });
        } catch (Exception exception) {
            routingContext.response().setStatusCode(400).putHeader(CONTENT_TYPE, Constant.APPLICATION_JSON).end(new JsonObject().put(Constant.STATUS, Constant.FAILED).encode());
        }

    }

    private void delete(RoutingContext routingContext) {
        try {
            String id = routingContext.pathParam(ID);

            Bootstrap.vertx.eventBus().<JsonObject>request(EVENTBUS_DATABASE, new JsonObject().put(METHOD, EVENTBUS_DELETECRED).put(CRED_ID, id), deletebyID -> {
                if (deletebyID.succeeded()) {
                    JsonObject deleteResult = deletebyID.result().body();
                    LOGGER.debug("Response {} ", deletebyID.result().body());
                    routingContext.response().setStatusCode(200).putHeader(CONTENT_TYPE, Constant.APPLICATION_JSON).end(deleteResult.encode());
                } else {
                    String result = deletebyID.cause().getMessage();
                    routingContext.response().setStatusCode(200).putHeader(CONTENT_TYPE, Constant.APPLICATION_JSON).end(result);
                }
            });

        } catch (Exception exception) {
            routingContext.response().setStatusCode(400).putHeader(CONTENT_TYPE, Constant.APPLICATION_JSON).end(new JsonObject().put(Constant.STATUS, Constant.FAILED).encode());
        }
    }

    private void getByID(RoutingContext routingContext) {
        try {
            String getId = routingContext.pathParam(ID);
            Bootstrap.vertx.eventBus().<JsonObject>request(EVENTBUS_DATABASE, new JsonObject().put(METHOD, EVENTBUS_GETCREDBYID).put(CRED_ID, getId), getbyIdHandler -> {
                if (getbyIdHandler.succeeded()) {
                    JsonObject getData = getbyIdHandler.result().body();
                    LOGGER.debug("Response {} ", getbyIdHandler.result().body());
                    routingContext.response().setStatusCode(200).putHeader(CONTENT_TYPE, Constant.APPLICATION_JSON).end(getData.encode());
                } else {
                    String result = getbyIdHandler.cause().getMessage();
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

            createData.put(METHOD, EVENTBUS_INSERTCRED);

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

            routingContext.response().setStatusCode(400).putHeader(CONTENT_TYPE, Constant.APPLICATION_JSON).end(new JsonObject().put(Constant.STATUS, Constant.FAILED).encode());

        }

    }

}
