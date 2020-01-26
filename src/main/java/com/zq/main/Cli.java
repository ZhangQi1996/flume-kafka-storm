package com.zq.main;

import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.api.RpcClient;
import org.apache.flume.api.RpcClientFactory;
import org.apache.flume.event.EventBuilder;
import java.nio.charset.StandardCharsets;
import java.util.Random;

public class Cli {

    public static void main(String[] args) {
        Random random = new Random();
        String[] info = {"[INFO]", "[ERROR]"};
        MyRpcClientFacade cli = new MyRpcClientFacade();
        cli.init("qc", 10000);
        try {
            for (int i = 1; i <= 10; i++) {
                String sendMsg = info[Math.abs(random.nextInt()) % 2] + "DAVID" + i;
                cli.sendDataToFlume(sendMsg);
                System.out.println(sendMsg);
            }
        } catch (Exception e) {
            e.printStackTrace();

        } finally {
            cli.cleanUp();
        }
    }
}

class MyRpcClientFacade {
    private RpcClient client;
    private String hostname;
    private int port;

    public void init(String hostname, int port) {
        // Setup the RPC connection
        this.hostname = hostname;
        this.port = port;
        // 默认是avro
        this.client = RpcClientFactory.getDefaultInstance(hostname, port);
        // Use the following method to create a thrift client (instead of the above line):
        // this.client = RpcClientFactory.getThriftInstance(hostname, port);
    }

    public void sendDataToFlume(String data) {
        // Create a Flume Event object that encapsulates the sample data
        Event event = EventBuilder.withBody(data, StandardCharsets.UTF_8);

        // Send the event
        try {
            client.append(event);
        } catch (EventDeliveryException e) {
            // clean up and recreate the client
            client.close();
            client = null;
            client = RpcClientFactory.getDefaultInstance(hostname, port);
            // Use the following method to create a thrift client (instead of the above line):
            // this.client = RpcClientFactory.getThriftInstance(hostname, port);
        }
    }

    public void cleanUp() {
        // Close the RPC connection
        client.close();
    }

}
