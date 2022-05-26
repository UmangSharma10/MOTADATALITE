package com.mindarray;

import com.mindarray.utility.Utility;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PollingEngine  extends AbstractVerticle {

    private static final Logger LOGGER = LoggerFactory.getLogger(PollingEngine.class);
    @Override
    public void start(Promise<Void> startPromise) throws Exception {
        Bootstrap.vertx.eventBus().<JsonObject>consumer(Constant.EVENTBUS_POLLING_ENGINE, pollingEngineHandler ->{

           JsonObject value = pollingEngineHandler.body();

           try {
               JsonObject pingResult = Utility.pingAvailability(value.getString(Constant.IP_ADDRESS));
               if (!pingResult.containsKey(Constant.ERROR)){
                   JsonObject result = Utility.spawning(value);
                   vertx.eventBus().request(Constant.EVENTBUS_DATADUMP, result, dataDump -> {
                       if (dataDump.succeeded()) {
                           pollingEngineHandler.reply(new JsonObject().put(Constant.STATUS, Constant.SUCCESS));
                       } else {
                           pollingEngineHandler.fail(-1, new JsonObject().put(Constant.STATUS, Constant.FAILED).encode());
                       }
                   });
               }
           }catch (Exception exception){
               LOGGER.error(exception.getMessage());

               pollingEngineHandler.fail(-1, new JsonObject().put(Constant.STATUS, Constant.FAILED).encode());
           }



        });
    }
}
