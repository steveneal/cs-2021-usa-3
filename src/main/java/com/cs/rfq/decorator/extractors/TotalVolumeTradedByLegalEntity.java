package com.cs.rfq.decorator.extractors;

import com.cs.rfq.decorator.Rfq;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.joda.time.DateTime;

import java.util.HashMap;
import java.util.Map;

import static com.cs.rfq.decorator.extractors.RfqMetadataFieldNames.*;

public class TotalVolumeTradedByLegalEntity implements RfqMetadataExtractor {

    long todayMs = DateTime.now().withMillisOfDay(0).getMillis();
    long pastWeekMs = DateTime.now().withMillis(todayMs).minusWeeks(1).getMillis();
    long pastYearMs = DateTime.now().withMillis(todayMs).minusYears(1).getMillis();



    @Override
    public Map<RfqMetadataFieldNames, Object> extractMetaData(Rfq rfq, SparkSession session, Dataset<Row> trades) {

        // create the time frames
//        long todayMs = DateTime.now().withMillisOfDay(0).getMillis();
//        long pastWeekMs = DateTime.now().withMillis(todayMs).minusWeeks(1).getMillis();
//        long pastYearMs = DateTime.now().withMillis(todayMs).minusYears(1).getMillis();

        // create the query strings
        String queryToday = String.format("SELECT sum(LastQty) from tradeToday where EntityId='%s' AND SecurityId='%s' AND TradeDate >= '%s'",
                rfq.getEntityId(),
                rfq.getIsin(),
                todayMs);

        String queryWeek = String.format("SELECT sum(LastQty) from tradeWeek where EntityId='%s' AND SecurityId='%s' AND TradeDate >= '%s'",
                rfq.getEntityId(),
                rfq.getIsin(),
                pastWeekMs);

        String queryYear = String.format("SELECT sum(LastQty) from tradeYear where EntityId='%s' AND SecurityId='%s' AND TradeDate >= '%s'",
                rfq.getEntityId(),
                rfq.getIsin(),
                pastYearMs);

        // execute and store the queries
        trades.createOrReplaceTempView("tradeToday");
        Dataset<Row> sqlQueryResultsToday = session.sql(queryToday);
        System.out.println(sqlQueryResultsToday.first().get(0));

        trades.createOrReplaceTempView("tradeWeek");
        Dataset<Row> sqlQueryResultsWeek = session.sql(queryWeek);
        System.out.println(sqlQueryResultsToday.count());

        trades.createOrReplaceTempView("tradeYear");
        Dataset<Row> sqlQueryResultsYear = session.sql(queryYear);
        System.out.println(sqlQueryResultsToday.count());

        // calculate the volume
        Object volumeToday = sqlQueryResultsToday.first().get(0);
        if (volumeToday == null) {
            volumeToday = 0L;
        }

        Object volumeWeek = sqlQueryResultsWeek.first().get(0);
        if (volumeWeek == null) {
            volumeWeek = 0L;
        }

        Object volumeYear = sqlQueryResultsYear.first().get(0);
        if (volumeYear == null) {
            volumeYear = 0L;
        }

// create a results hashmap and store results
        Map<RfqMetadataFieldNames, Object> results = new HashMap<>();
        results.put(RfqMetadataFieldNames.tradesWithEntityToday, volumeToday);
        results.put(RfqMetadataFieldNames.tradesWithEntityPastWeek, volumeWeek);
        results.put(RfqMetadataFieldNames.tradesWithEntityPastYear, volumeYear);
        return results;
    }
    protected void setDate() {
        todayMs = DateTime.now().withMillisOfDay(0).getMillis();
        pastWeekMs = DateTime.now().withMillis(todayMs).minusYears(3).minusWeeks(4).getMillis();
        pastYearMs = DateTime.now().withMillis(todayMs).minusYears(3).getMillis();
    }

}
