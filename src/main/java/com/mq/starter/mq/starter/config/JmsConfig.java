package com.mq.starter.mq.starter.config;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.command.ActiveMQQueue;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.jms.DefaultJmsListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.jms.config.DefaultJmsListenerContainerFactory;
import org.springframework.jms.config.JmsListenerContainerFactory;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.jms.support.converter.MappingJackson2MessageConverter;
import org.springframework.jms.support.converter.MessageConverter;
import org.springframework.jms.support.converter.MessageType;

import javax.jms.ConnectionFactory;
import javax.jms.Queue;

@Configuration
public class JmsConfig {

    @Value("${jms.activemq.request.queue}")
    String requestQueue;

    @Value("${jms.activemq.response.queue}")
    String responseQueue;

    //MQ server 1 Config
    @Value("${jms1.activemq.broker.url}")
    String brokerUrl1;

    @Value("${jms1.activemq.borker.username}")
    String userName1;

    @Value("${jms1.activemq.borker.password}")
    String password1;

    //MQ server 2 Config
    @Value("${jms2.activemq.broker.url}")
    String brokerUrl2;

    @Value("${jms2.activemq.borker.username}")
    String userName2;

    @Value("${jms2.activemq.borker.password}")
    String password2;

    @Bean
    public Queue reqQueue(){
        return new ActiveMQQueue(requestQueue);
    }

    @Bean
    public Queue resQueue(){
        return new ActiveMQQueue(responseQueue);
    }

    //@Bean
    public MessageConverter messageConverter() {
        MappingJackson2MessageConverter converter = new MappingJackson2MessageConverter();
        converter.setTargetType(MessageType.TEXT);
        converter.setTypeIdPropertyName("_type");
        return converter;
    }

    @Bean
    @Primary
    public ConnectionFactory connFactory(){
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory();
        factory.setBrokerURL(brokerUrl1);
        factory.setUserName(userName1);
        factory.setPassword(password1);
        return factory;
    }

    @Bean
    public ConnectionFactory connFactory2(){
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory();
        factory.setBrokerURL(brokerUrl2);
        factory.setUserName(userName2);
        factory.setPassword(password2);
        return factory;
    }

    @Bean
    public JmsListenerContainerFactory<?> jmsFactory1(ConnectionFactory connectionFactory,
                                                      DefaultJmsListenerContainerFactoryConfigurer configurer) {
        DefaultJmsListenerContainerFactory factory = new DefaultJmsListenerContainerFactory();
        configurer.configure(factory, connFactory());
        return factory;
    }

    @Bean
    public JmsListenerContainerFactory<?> jmsFactory2(ConnectionFactory connectionFactory,
                                                      DefaultJmsListenerContainerFactoryConfigurer configurer) {
        DefaultJmsListenerContainerFactory factory = new DefaultJmsListenerContainerFactory();
        configurer.configure(factory, connFactory2());
        return factory;
    }

    @Bean
    public JmsTemplate jmsTemplate(){
        return new JmsTemplate(connFactory());
    }

    @Bean
    public JmsTemplate jmsTemplate2(){
        return new JmsTemplate(connFactory2());
    }
}
