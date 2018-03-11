package com.mq.starter.mq.starter.resources;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jms.annotation.JmsListener;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.jms.core.MessagePostProcessor;
import org.springframework.jms.support.JmsHeaders;
import org.springframework.messaging.handler.annotation.Header;
import java.util.Random;

import javax.jms.JMSException;
import javax.jms.Message;

import javax.jms.Queue;

@RestController
public class ProducerResource {

    @Autowired
    private JmsTemplate jmsTemplate;

    @Autowired
    private JmsTemplate jmsTemplate2;

    @Autowired
    private Queue resQueue;

    @Autowired
    private Queue reqQueue;

    @JmsListener(destination = "standalone.request", containerFactory = "jmsFactory1")
    public void consume(String msg, @Header(JmsHeaders.CORRELATION_ID) String corrId) throws InterruptedException {

        Thread.sleep(3000);
        int rand  = new Random().nextInt(2);
        System.out.println("Recieved message from Server1 and " + rand+"   random value");
        if(rand == 0){
            jmsTemplate.convertAndSend(resQueue, msg,
                    new CorrelationIdPostProcessor(corrId));
            System.out.println("Got a message from Server1 and republished to response queue of server1 ---> "+msg+"  "+corrId);
        }else {
            jmsTemplate2.convertAndSend(resQueue, msg,
                    new CorrelationIdPostProcessor(corrId));
            System.out.println("Got a message from Server1 and republished to response queue of server2 ---> "+msg+"  "+corrId);
        }
    }

    @JmsListener(destination = "standalone.request", containerFactory = "jmsFactory2")
    public void consumeServer2(String msg, @Header(JmsHeaders.CORRELATION_ID) String corrId) throws InterruptedException {

        Thread.sleep(3000);
        int rand  = new Random().nextInt(2);
        System.out.println("Recieved message from Server2 and " + rand+"   random value");
        if(rand == 0){
            jmsTemplate.convertAndSend(resQueue, msg,
                    new CorrelationIdPostProcessor(corrId));
            System.out.println("Got a message from Server2 and republished to response queue of server1 ---> "+msg+"  "+corrId);
        }else {
            jmsTemplate2.convertAndSend(resQueue, msg,
                    new CorrelationIdPostProcessor(corrId));
            System.out.println("Got a message from Server2 and republished to response queue of server2 ---> "+msg+"  "+corrId);
        }
    }

    private class CorrelationIdPostProcessor implements MessagePostProcessor {
        private final String correlationId;

        public CorrelationIdPostProcessor(final String correlationId) {
            this.correlationId = correlationId;
        }

        @Override
        public Message postProcessMessage(final Message msg)
                throws JMSException {
            msg.setJMSCorrelationID(correlationId);
            return msg;
        }
    }
}
