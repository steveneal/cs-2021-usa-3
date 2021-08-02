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

public class AveragePrice implements RfqMetadataExtractor {
    //  average price traded by the bank over the past week for all instruments

    @Override
    public Map<RfqMetadataFieldNames, Object> extractMetaData(Rfq rfq, SparkSession session, Dataset<Row> trades) {

        Date date = new Date();
        long todayMs = date.getTime();
        long pastWeekMs = DateTime.now().withMillis(todayMs).minusWeeks(1).getMillis();

        Dataset<Row> filtered = trades
                .filter(trades.col("SecurityId").equalTo(rfq.getIsin()))
                .filter(trades.col("EntityId").equalTo(rfq.getEntityId()))
                .filter(trades.col("TradeDate").$greater(new java.sql.Date(pastWeekMs)))
                .agg(avg(trades.col("LastPx")));


        Map<RfqMetadataFieldNames, Object> results = new HashMap<>();


        results.put(averageTradePriceWeekly, trades);
        return results;
    }


}
