package org.example.flink.spendreport.datagen.model;

import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.Iterator;
import java.util.Random;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;

/** A supplier that generates an arbitrary transaction. */
@Component
public class TransactionSupplier implements Supplier<Transaction> {

    private static final Random generator = new Random();

    private final Iterator<Long> accounts =
            Stream.generate(() -> Stream.of(1L, 2L, 3L, 4L, 5L))
            .flatMap(UnaryOperator.identity())
            .iterator();

    private final Iterator<LocalDateTime> timestamps =
            Stream.iterate(
                    LocalDateTime.of(2000, 1, 1, 1, 0),
                    time -> time.plusMinutes(5).plusSeconds(generator.nextInt(58) + 1))
                    .iterator();

    @Override
    public Transaction get() {
        Transaction transaction = new Transaction();
        transaction.accountId = accounts.next();
        transaction.amount = generator.nextInt(1000);
        transaction.timestamp = timestamps.next();
        return transaction;
    }

}
