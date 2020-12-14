package org.example.flink.spendreport.datagen.model;

import lombok.Data;
import java.time.LocalDateTime;


@Data
public class Transaction {

    public long accountId;

    public int amount;

    public LocalDateTime timestamp;
}
