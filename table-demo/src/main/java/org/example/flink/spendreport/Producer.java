package org.example.flink.spendreport;

import lombok.extern.slf4j.Slf4j;
import org.example.flink.spendreport.datagen.model.Transaction;
import org.example.flink.spendreport.datagen.model.TransactionSupplier;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import javax.annotation.PostConstruct;

@Slf4j
@Component
public class Producer {

    @Value("${spring.kafka.consumer.topic}")
    private String topic;

    @Autowired
    private KafkaTemplate<Long, Transaction> kafkaTemplate;

    @Autowired
    TransactionSupplier supplier;

    public void send(Transaction transaction) {
        log.info("准备发送消息为：{}", transaction);

        ListenableFuture<SendResult<Long, Transaction>> future = kafkaTemplate.send(topic, transaction);
        future.addCallback(new ListenableFutureCallback<SendResult<Long, Transaction>>() {
            @Override
            public void onFailure(Throwable ex) {
                log.info("topic: {} - 生产者 发送消息失败：{}", topic, ex.getMessage());
            }

            @Override
            public void onSuccess(SendResult<Long, Transaction> result) {
                log.info("topic: {} - 生产者 发送消息成功：{}", topic, result);
            }
        });
    }

    @PostConstruct
    public void generateTransaction() {
        while (true) {
            Transaction transaction = supplier.get();
            send(transaction);
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
