package com.cs.rfq.decorator.extractors;

import com.cs.rfq.decorator.Rfq;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.joda.time.DateTime;

import java.util.HashMap;
import java.util.Map;

import static com.cs.rfq.decorator.extractors.RfqMetadataFieldNames.*;

public class TotalVolumeTradedByInstrument implements RfqMetadataExtractor{

    @Override
    public Map<RfqMetadataFieldNames, Object> extractMetaData(Rfq rfq, SparkSession session, Dataset<Row> trades) {

        // create the time frames
        long todayMs = DateTime.now().withMillisOfDay(0).getMillis();
        long pastWeekMs = DateTime.now().withMillis(todayMs).minusWeeks(1).getMillis();
        long pastYearMs = DateTime.now().withMillis(todayMs).minusYears(1).getMillis();

        // create the query strings
        String queryToday = String.format("SELECT sum(LastQty) from trade where SecurityId='%s' AND TradeDate >= '%s'",
                rfq.getIsin(),
                todayMs);

        String queryWeek = String.format("SELECT sum(LastQty) from trade where SecurityId='%s' AND TradeDate >= '%s'",
                rfq.getIsin(),
                pastWeekMs);

        String queryYear = String.format("SELECT sum(LastQty) from trade where SecurityId='%s' AND TradeDate >= '%s'",
                rfq.getIsin(),
                pastYearMs);

        // execute and store the queries
        trades.createOrReplaceTempView("tradeToday");
        Dataset<Row> sqlQueryResultsToday = session.sql(queryToday);

        trades.createOrReplaceTempView("tradeWeek");
        Dataset<Row> sqlQueryResultsWeek = session.sql(queryWeek);

        trades.createOrReplaceTempView("tradeYear");
        Dataset<Row> sqlQueryResultsYear = session.sql(queryYear);

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
        results.put(RfqMetadataFieldNames.tradesBySecurityToday, volumeToday);
        results.put(RfqMetadataFieldNames.tradesBySecurityPastWeek, volumeWeek);
        results.put(RfqMetadataFieldNames.tradesBySecurityPastYear, volumeYear);
        return results;
    }



//    @Override
//    public Map<RfqMetadataFieldNames, Object> extractMetaData(Rfq rfq, SparkSession session, Dataset<Row> trades) {
//
//        long todayMs = DateTime.now().withMillisOfDay(0).getMillis();
//        long pastWeekMs = DateTime.now().withMillis(todayMs).minusWeeks(1).getMillis();
//        long pastYearMs = DateTime.now().withMillis(todayMs).minusYears(1).getMillis();
//
//        Dataset<Row> filtered = trades
//                .filter(trades.col("SecurityId").equalTo(rfq.getIsin()));
//
//        long tradesToday = filtered.filter(trades.col("TradeDate").$greater$eq(new java.sql.Date(todayMs))).count();
//        long tradesPastWeek = filtered.filter(trades.col("TradeDate").$greater$eq(new java.sql.Date(pastWeekMs))).count();
//        long tradesPastYear = filtered.filter(trades.col("TradeDate").$greater$eq(new java.sql.Date(pastYearMs))).count();
//
//        Map<RfqMetadataFieldNames, Object> results = new HashMap<>();
//        results.put(tradesWithEntityToday, tradesToday);
//        results.put(tradesWithEntityPastWeek, tradesPastWeek);
//        results.put(tradesWithEntityPastYear, tradesPastYear);
//        return results;
//    }

}