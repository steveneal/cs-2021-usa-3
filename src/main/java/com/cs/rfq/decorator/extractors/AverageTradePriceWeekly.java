package com.cs.rfq.decorator.extractors;

import com.cs.rfq.decorator.Rfq;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import static org.apache.spark.sql.functions.avg;
import org.apache.spark.sql.SparkSession;
import org.joda.time.DateTime;

import java.util.Date;

import java.util.HashMap;
import java.util.Map;

import static com.cs.rfq.decorator.extractors.RfqMetadataFieldNames.*;

public class AverageTradePriceWeekly implements RfqMetadataExtractor {
    //  average price traded by the bank over the past week for all instruments
    private String since;

    public AverageTradePriceWeekly() {
        this.since = DateTime.now().getYear() + "-01-01";
    }

    @Override
    public Map<RfqMetadataFieldNames, Object> extractMetaData(Rfq rfq, SparkSession session, Dataset<Row> trades) {

        Date date = new Date();
        long todayMs = date.getTime();
        long pastWeekMs = DateTime.now().withMillis(todayMs).minusWeeks(1).getMillis();

        Dataset<Row> filtered = trades
                .filter(trades.col("SecurityId").equalTo(rfq.getIsin()))
                .filter(trades.col("EntityId").equalTo(rfq.getEntityId()))
                .filter(trades.col("TradeDate").$greater(since))
                .agg(avg(trades.col("LastPx").as("AveragePx")));

    System.out.println(filtered.toString());
//        filtered.show();
        Object volume = 0.0;
//        Object volume = filtered.first().get(0);
//        System.out.println( .getClass());
        if (filtered.first().get(0) != null) {
            System.out.println("I GOT HIT!");
            volume = filtered.first().get(0);;
        }
//        System.out.println("VOLUME POST CHECK " + volume.toString());
        Map<RfqMetadataFieldNames, Object> results = new HashMap<>();
        results.put(averageTradePriceWeekly, (int) Math.round((double) volume) );
        return results;
    }

    protected void setSince(String since) {
        this.since = since;
    }
}
