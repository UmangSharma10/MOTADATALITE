package com.mindarray.utility;

import com.mindarray.APIServer;
import com.mindarray.Constant;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Utility {
    private static final Logger LOGGER = LoggerFactory.getLogger(APIServer.class);

    private static final String IPV4_REGEX =
            "^(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\." +
                    "(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\." +
                    "(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\." +
                    "(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$";

    private static final Pattern IPv4_PATTERN = Pattern.compile(IPV4_REGEX);

    private static final String PORTREGEX = "^((6553[0-5])|(655[0-2][0-9])|(65[0-4][0-9]{2})|(6[0-4][0-9]{3})|([1-5][0-9]{4})|([0-5]{0,5})|([0-9]{1,4}))$";
    private static final Pattern PORT = Pattern.compile(PORTREGEX);

    public static boolean isValidPort(String port) {
        if (port == null) {
            return false;
        }

        Matcher matcher = PORT.matcher(port);

        return matcher.matches();
    }

    public static boolean isValidIp(String ip) {
        if (ip == null) {
            return false;
        }

        Matcher matcher = IPv4_PATTERN.matcher(ip);

        return matcher.matches();
    }

    public  static JsonObject  pingAvailability(String ip) throws Exception {
        JsonObject ping = new JsonObject();
        HashMap<String, String> myMap = new HashMap<>();
        ArrayList<String> commandList = new ArrayList<>();

        commandList.add("fping");

        commandList.add("-q");

        commandList.add("-c");

        commandList.add("3");

        commandList.add("-t");

        commandList.add("1000");

        commandList.add(ip);

        ProcessBuilder build = new ProcessBuilder(commandList);

        Process process = build.start();

        // to read the output
        BufferedReader input = new BufferedReader(new InputStreamReader(process.getInputStream()));

        BufferedReader Error = new BufferedReader(new InputStreamReader(process.getErrorStream()));

        String readPing;

        while ((readPing = input.readLine()) != null) {
            LOGGER.debug(readPing);
        }

        LOGGER.debug("error (if any): ");
        while ((readPing = Error.readLine()) != null) {
            LOGGER.debug(readPing);

            String[] s1 = readPing.split(":");

            String[] s2 = s1[1].split(",");

            String[] s3 = s2[0].split("=");

            if (s2.length == 2) {

                String[] loss = s3[1].split("/");

                myMap.put("packetxmt", loss[0]);
                myMap.put("packetrcv", loss[1]);

            } else if (s2.length == 1) {
                myMap.put("packetrcv", "0");

            }

        }
        if (myMap.get("packetrcv").equals("3")) {
            ping.put(Constant.STATUS, Constant.UP);
        } else {
            ping.put(Constant.STATUS, Constant.DOWN);
        }
        input.close();
        Error.close();


        return ping;
    }


    public static JsonObject spawning(JsonObject pluginJson) throws IOException {
        JsonObject result = new JsonObject();

        BufferedReader stdInput = null;

        BufferedReader stdError = null;

        try {
            List<String> commands = new ArrayList<>();

            commands.add(System.getProperty("user.dir") + System.getProperty("file.separator") + "./pluginengine");

            String encodedString = Base64.getEncoder().encodeToString(pluginJson.encode().getBytes(StandardCharsets.UTF_8));

            commands.add(encodedString);

            ProcessBuilder processBuilder = new ProcessBuilder(commands);

            Process process = processBuilder.start();

            stdInput = new BufferedReader(new InputStreamReader(process.getInputStream()));

            stdError = new BufferedReader(new InputStreamReader(process.getErrorStream()));

            String readInput;

            String decoder;

            while ((readInput = stdInput.readLine()) != null) {
                byte[] decodedBytes = Base64.getDecoder().decode(readInput);
                decoder = new String(decodedBytes);
                result = new JsonObject(decoder);

            }
            while ((readInput = stdError.readLine()) != null) {
                byte[] decodedBytes = Base64.getDecoder().decode(readInput);
                decoder = new String(decodedBytes);
                result = new JsonObject(decoder);

            }
            result.remove("category");
        } catch (IOException exception) {

            LOGGER.error(exception.getMessage());
        } finally {
            if (stdInput != null) {
                stdInput.close();
            }
            if (stdError != null) {
                stdError.close();
            }
        }

        return result;

    }

}



