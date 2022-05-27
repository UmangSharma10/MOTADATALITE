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
        Bootstrap.vertx.eventBus().<JsonObject>consumer(Constant.EVENTBUS_POLLING_ENGINE, pollingEngineHandler ->{

            JsonObject value = pollingEngineHandler.body();

            Bootstrap.vertx.executeBlocking(pollerBlocking ->{
                   try {
                       JsonObject pingResult = Utility.pingAvailability(value.getString(Constant.IP_ADDRESS));
                       if (!pingResult.containsKey(Constant.ERROR)){
                           pollerBlocking.complete(pingResult);
                       }
                       else {
                           pollerBlocking.fail( new JsonObject().put(Constant.STATUS, Constant.FAILED).encode());
                       }

                   }
                   catch (Exception exception){
                       pollerBlocking.fail(new JsonObject().put(Constant.STATUS, Constant.FAILED).encode());
                   }
               }).onComplete(onCompletePollerHandler -> {
                   if (onCompletePollerHandler.succeeded()){
                       try {
                          JsonObject result = Utility.spawning(value);
                           vertx.eventBus().request(Constant.EVENTBUS_DATADUMP, result, dataDump -> {
                               if (dataDump.succeeded()) {
                                   pollingEngineHandler.reply(new JsonObject().put(Constant.STATUS, Constant.SUCCESS));
                               } else {
                                   pollingEngineHandler.fail(-1, new JsonObject().put(Constant.STATUS, Constant.FAILED).encode());
                               }
                           });
                       } catch (Exception exception) {
                           pollingEngineHandler.fail(-1, new JsonObject().put(Constant.STATUS, Constant.FAILED).encode());
                       }

                   }
               });

        });
      startPromise.complete();
    }
}
