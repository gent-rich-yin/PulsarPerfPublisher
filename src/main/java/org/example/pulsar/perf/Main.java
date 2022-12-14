package org.example.pulsar.perf;

import org.apache.pulsar.client.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;

import java.sql.Array;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

@SpringBootApplication
public class Main {
    static Logger logger = LoggerFactory.getLogger(Main.class);
    static int NUM_OF_MESSAGES = 1000;

    PulsarClient pulsarClient;
    Producer<String> pulsarProducer;
    @Value("${PULSAR.SERVERS}")
    String pulsarServers;
    String[] messages;
    int currentMessageIndex = 0;

    public static void main(String[] args) {
        SpringApplication.run(Main.class, args);
    }

    public void initClient() throws PulsarClientException {
        pulsarClient = PulsarClient.builder()
                .serviceUrl(pulsarServers)
                .build();
    }

    @EventListener(ApplicationReadyEvent.class)
    public void startPublishing() throws Exception {
        initClient();

        String currentTopic = null;
        int currentMessageSize = 0;
        long stime;

        messages = generateRandomStrings();
        while(true) {
            if( currentTopic == null || !currentTopic.equals(PerfStates.topic)
                    || currentMessageSize == 0 || currentMessageSize != PerfStates.messageSize ) {
                currentTopic = PerfStates.topic;
                currentMessageSize = PerfStates.messageSize;
                if( currentTopic != null && !currentTopic.equals("") && currentMessageSize > 0 ) {
                    updatePerfMessage("Start generating random strings");
                    messages = generateRandomStrings();
                    sleep(1000);
                    updatePerfMessage("Done generating random strings");
                    sleep(1000);
                    updatePerfMessage("Start publishing...");
                    sleep(1000);
                }
            }

            if( currentTopic == null || currentTopic.equals("") || currentMessageSize <= 0 ) {
                updatePerfMessage("Waiting for valid config assignment");
                sleep(1000);
                continue;
            }

            stime = System.currentTimeMillis();
            int count = 1;
            int messagesSentLastSecond = 0;
            pulsarProducer = pulsarClient.newProducer(Schema.STRING)
                    .topic(PerfStates.topic)
                    .enableBatching(true)
                    .blockIfQueueFull(true)
                    .batchingMaxMessages(1000)
                    .batchingMaxBytes(1000000)
                    .create();
            while( currentTopic.equals(PerfStates.topic)
                    && currentMessageSize == PerfStates.messageSize ) {
//                List<CompletableFuture<MessageId>> list = new ArrayList<>();
                pulsarProducer.newMessage()
                        .key(Integer.toString(count++))
                        .value(messages[currentMessageIndex++])
                        .sendAsync();
                if( currentMessageIndex >= NUM_OF_MESSAGES ) {
                    currentMessageIndex = 0;
                }
                messagesSentLastSecond++;
                long ftime = System.currentTimeMillis();
                if( ftime - stime > 1000 ) {
                    long sentTime = ftime - stime;
//                    list.forEach(f -> {
//                        try {
//                            f.get();
//                        } catch (InterruptedException e) {
//                            throw new RuntimeException(e);
//                        } catch (ExecutionException e) {
//                            throw new RuntimeException(e);
//                        }
//                    });
//                    long flushTime = System.currentTimeMillis() - ftime;
                    updatePerfMessage("messagesSentLastSecond: {0}, sentTime: {1}", messagesSentLastSecond, sentTime);
                    stime = System.currentTimeMillis();
                    messagesSentLastSecond = 0;
                }
            }
            pulsarProducer.close();
        }
    }

    private static void updatePerfMessage(String s, Object... args) {
        PerfStates.perfMessage = MessageFormat.format(s, args);
        logger.info(PerfStates.perfMessage);
    }

    public static String generateRandomString() {
        char[] letters = "abcdefghijklmnopqrstuvwxyz".toCharArray();
        StringBuilder builder = new StringBuilder();
        for(int i = 0; i< PerfStates.messageSize; i++ ) {
            int index = (int) (Math.random() * 26);
            builder.append(letters[index]);
        }
        return builder.toString();
    }

    public static String[] generateRandomStrings() {
        String[] strings = new String[NUM_OF_MESSAGES];
        for( int i=0; i<strings.length; i++ ) {
            strings[i] = generateRandomString();
        }
        return strings;
    }

    public static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

}
