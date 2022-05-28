package com.mindarray;

import com.mindarray.utility.Utility;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class PollingEngine  extends AbstractVerticle {

    private static final Logger LOGGER = LoggerFactory.getLogger(PollingEngine.class);
    @Override
    public void start(Promise<Void> startPromise) throws Exception {
        Bootstrap.vertx.eventBus().<JsonObject>localConsumer(Constant.EVENTBUS_POLLING_ENGINE, pollingEngineHandler ->{

            JsonObject value = pollingEngineHandler.body();

            Bootstrap.vertx.executeBlocking(pollerBlocking ->{
                   try {
                       JsonObject pingResult = Utility.pingAvailability(value.getString(Constant.IP_ADDRESS));
                       if (pingResult.getString(Constant.STATUS).equals(Constant.UP)){
                           pollerBlocking.complete(pingResult);
                       }
                       else {
                           pollerBlocking.fail( new JsonObject().put("PING", Constant.FAILED).encode());
                       }

                   }
                   catch (Exception exception){
                       pollerBlocking.fail(new JsonObject().put("PING", Constant.FAILED).encode());
                   }
               }).onComplete(onCompletePollerHandler -> {
                   if (onCompletePollerHandler.succeeded()){
                       try {
                          JsonObject result = Utility.spawning(value);
                              vertx.eventBus().request(Constant.EVENTBUS_DATADUMP, result, dataDump -> {
                                  if (dataDump.succeeded()) {
                                     LOGGER.debug("DATA DUMPED");
                                  } else {
                                     LOGGER.debug("DATA NOT DUMPED");
                                  }
                              });

                       } catch (Exception exception) {
                           LOGGER.debug(exception.getMessage());
                       }

                   }
                   else {
                      LOGGER.debug("DATA NOT DUMPED");
                   }
               });

        });
      startPromise.complete();
    }
}
