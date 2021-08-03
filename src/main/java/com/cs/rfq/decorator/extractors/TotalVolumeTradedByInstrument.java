package com.cs.rfq.decorator.extractors;

import com.cs.rfq.decorator.Rfq;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.HashMap;
import java.util.Map;

public class TotalVolumeTradedByInstrument implements RfqMetadataExtractor{

    private String today = java.time.LocalDate.now().toString();
    private String pastWeek = java.time.LocalDate.now().minusWeeks(1).toString();
    private String pastYear = java.time.LocalDate.now().minusYears(1).toString();


    @Override
    public Map<RfqMetadataFieldNames, Object> extractMetaData(Rfq rfq, SparkSession session, Dataset<Row> trades) {

        // create the query strings
        String queryToday = String.format("SELECT sum(LastQty) from tradeToday where SecurityId='%s' AND TradeDate >= '%s'",
                rfq.getIsin(),
                today);

        String queryWeek = String.format("SELECT sum(LastQty) from tradeWeek where SecurityId='%s' AND TradeDate >= '%s'",
                rfq.getIsin(),
                pastWeek);

        String queryYear = String.format("SELECT sum(LastQty) from tradeYear where SecurityId='%s' AND TradeDate >= '%s'",
                rfq.getIsin(),
                pastYear);

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



    protected void setDate(int dayLag, int yearLag, int weekLag) {
        // sets the date for the program in hindsight to work with test data
         today = java.time.LocalDate.now().minusDays(dayLag).toString();
         pastWeek = java.time.LocalDate.now().minusWeeks(weekLag).toString();
         pastYear = java.time.LocalDate.now().minusYears(yearLag).toString();
    }

}