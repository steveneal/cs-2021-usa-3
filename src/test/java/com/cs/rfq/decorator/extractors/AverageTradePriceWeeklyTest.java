package com.cs.rfq.decorator.extractors;

import com.cs.rfq.decorator.Rfq;
import com.cs.rfq.decorator.TradeDataLoader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class AverageTradePriceWeeklyTest extends AbstractSparkUnitTest {

    private Rfq rfq;
    Dataset<Row> trades;

    @BeforeEach
    public void setup() {
        rfq = new Rfq();
        rfq.setEntityId(5561279226039690843L);
        rfq.setIsin("AT0000A0VRQ6");
//        rfq.setPrice(138.0);

        String filePath = getClass().getResource("volume-traded-1.json").getPath();
        trades = new TradeDataLoader().loadTrades(session, filePath);
    }

    @Test
    public void checkAverageWhenAllTradesMatch() {

        AverageTradePriceWeekly extractor = new AverageTradePriceWeekly();
        extractor.setSince("2000-01-01");

        Map<RfqMetadataFieldNames, Object> meta = extractor.extractMetaData(rfq, session, trades);

        Object result = meta.get(RfqMetadataFieldNames.averageTradePriceWeekly);

        assertEquals(138, result);
    }

    @Test
    public void checkVolumeWhenNoTradesMatch() {

        //all test trade data are for 2018 so this will cause no matches
        AverageTradePriceWeekly extractor = new AverageTradePriceWeekly();
//        extractor.setSince("2018-01-01");

        Map<RfqMetadataFieldNames, Object> meta = extractor.extractMetaData(rfq, session, trades);

        Object result = meta.get(RfqMetadataFieldNames.averageTradePriceWeekly);

        assertEquals(0, result);
    }
}
