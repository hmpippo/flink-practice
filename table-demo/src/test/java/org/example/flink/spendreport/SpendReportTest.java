package org.example.flink.spendreport;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.junit4.SpringRunner;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * A unit test of the spend report.
 * If this test passes then the business
 * logic is correct.
 */
@RunWith(SpringRunner.class)
public class SpendReportTest {

    private static final LocalDateTime DATE_TIME = LocalDateTime.of(2020, 1, 1, 0, 0);

    @Test
    public void testReport() {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();
        TableEnvironment tEnv = TableEnvironment.create(settings);

        Table transactions =
                tEnv.fromValues(
                        DataTypes.ROW(
                                DataTypes.FIELD("account_id", DataTypes.BIGINT()),
                                DataTypes.FIELD("amount", DataTypes.BIGINT()),
                                DataTypes.FIELD("transaction_time", DataTypes.TIMESTAMP(3))),
                        Row.of(1, 188, DATE_TIME.plusMinutes(12)),
                        Row.of(2, 374, DATE_TIME.plusMinutes(47)),
                        Row.of(3, 112, DATE_TIME.plusMinutes(36)),
                        Row.of(4, 478, DATE_TIME.plusMinutes(3)),
                        Row.of(5, 208, DATE_TIME.plusMinutes(8)),
                        Row.of(1, 379, DATE_TIME.plusMinutes(53)),
                        Row.of(2, 351, DATE_TIME.plusMinutes(32)),
                        Row.of(3, 320, DATE_TIME.plusMinutes(31)),
                        Row.of(4, 259, DATE_TIME.plusMinutes(19)),
                        Row.of(5, 273, DATE_TIME.plusMinutes(42)));

        try {
            TableResult results = SpendReport.report(transactions).execute();

            MatcherAssert.assertThat(
                    materialize(results),
                    Matchers.containsInAnyOrder(
                            Row.of(1L, DATE_TIME, 567L),
                            Row.of(2L, DATE_TIME, 725L),
                            Row.of(3L, DATE_TIME, 432L),
                            Row.of(4L, DATE_TIME, 737L),
                            Row.of(5L, DATE_TIME, 481L)));
        } catch (UnimplementedException e) {
            Assume.assumeNoException("The walkthrough has not been implemented", e);
        }
    }

    private static List<Row> materialize(TableResult results) {
        try (CloseableIterator<Row> resultIterator = results.collect()) {
            return StreamSupport
                    .stream(Spliterators.spliteratorUnknownSize(resultIterator, Spliterator.ORDERED), false)
                    .collect(Collectors.toList());
        } catch (Exception e) {
            throw new RuntimeException("Failed to materialize results", e);
        }
    }
}