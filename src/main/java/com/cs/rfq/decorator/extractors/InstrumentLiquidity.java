package com.cs.rfq.decorator.extractors;

import com.cs.rfq.decorator.Rfq;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.joda.time.DateTime;

import java.util.HashMap;
import java.util.Map;

import static com.cs.rfq.decorator.extractors.RfqMetadataFieldNames.*;

public class InstrumentLiquidity implements RfqMetadataExtractor {
    private String since;

    public InstrumentLiquidity() {
        this.since = DateTime.now().toString();
    }

    @Override
    public Map<RfqMetadataFieldNames, Object> extractMetaData(Rfq rfq, SparkSession session, Dataset<Row> trades) {

        String timePeriod = DateTime.now().minusMonths(6).toString().substring(0,10);

        String pastMonthData =
                String.format("SELECT sum(LastQty) " +
                                "FROM trade " +
                                "WHERE TraderId='%s' " +
                                "AND SecurityID='%s' " +
                                "AND TradeDate >= '%s'",
                rfq.getTraderId(),
                rfq.getIsin(),
                timePeriod);

        trades.createOrReplaceTempView("trade");
        Dataset<Row> volWeekResults = session.sql(pastMonthData);

        Map<RfqMetadataFieldNames, Object> results = new HashMap<>();
        if (volWeekResults.first().get(0) != null) {
            results.put(instrumentLiquidity, volWeekResults.first().get(0));
        }
        else {
            results.put(instrumentLiquidity, 0);
        }

        return results;
    }
}