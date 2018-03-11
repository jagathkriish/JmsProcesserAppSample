package com.mq.starter.mq.starter.resources;

import org.springframework.jms.annotation.JmsListener;
import org.springframework.stereotype.Component;

@Component
public class Listener {
    //@JmsListener(destination = "standalone.queue")
    public void consume(String msg){
        System.out.println("Got a message ---> "+msg);
    }
}
