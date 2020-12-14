package org.example.flink.spendreport.datagen.model;

import org.apache.kafka.common.serialization.Serializer;
import java.time.format.DateTimeFormatter;

/** Serializes a {@link Transaction} into a CSV record. */
public class TransactionSerializer implements Serializer<Transaction> {

    private static final DateTimeFormatter formatter =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    @Override
    public byte[] serialize(String topic, Transaction transaction) {
        String csv =
                String.format(
                        "%s, %s, %s",
                        transaction.accountId, transaction.amount, transaction.timestamp.format(formatter));
        return csv.getBytes();
    }
}
