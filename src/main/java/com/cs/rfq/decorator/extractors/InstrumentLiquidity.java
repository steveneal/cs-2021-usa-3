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

        long timePeriod = DateTime.now().minusMonths(1).getMillis();

        String pastMonthData =
                String.format("SELECT sum(LastQty) " +
                                "FROM trade " +
                                "WHERE EntityId='%s' " +
                                "AND SecurityID='%s' " +
                                "AND TradeDate >= '%s'",
                rfq.getEntityId(),
                rfq.getIsin(),
                timePeriod);

        System.out.println("pastMonthData: "+pastMonthData);

        trades.createOrReplaceTempView("trade");
        Dataset<Row> thisMonthResults = session.sql(pastMonthData);

        System.out.println("thisMonthResults: "+thisMonthResults.first().get(0));

        Map<RfqMetadataFieldNames, Object> results = new HashMap<>();
        if (thisMonthResults.first().get(0) != null) {
            results.put(instrumentLiquidity, thisMonthResults.first().get(0));
        }
        else {
            results.put(instrumentLiquidity, 0);
        }

        return results;
    }
    protected void setSince(String since) {
        this.since = since;
    }
}