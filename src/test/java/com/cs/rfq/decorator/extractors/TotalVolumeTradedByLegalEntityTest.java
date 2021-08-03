package com.cs.rfq.decorator.extractors;

import com.cs.rfq.decorator.Rfq;
import com.cs.rfq.decorator.TradeDataLoader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TotalVolumeTradedByLegalEntityTest extends AbstractSparkUnitTest {

    private Rfq rfq;
    private Dataset<Row> trades;

    @BeforeEach
    private void setup() {
        rfq = new Rfq();
        rfq.setEntityId(5561279226039690843L);
        rfq.setIsin("AT0000A0VRQ6");

        String filePath = getClass().getResource("volume-traded-1.json").getPath();
        trades = new TradeDataLoader().loadTrades(session, filePath);
    }



    @Test
    private void checkDailyNoMatch() {

        //all test trade data are for 2018 so this will cause no matches
        TotalVolumeTradedByLegalEntity extractor = new TotalVolumeTradedByLegalEntity();

        Map<RfqMetadataFieldNames, Object> meta = extractor.extractMetaData(rfq, session, trades);

        Object result = meta.get(RfqMetadataFieldNames.tradesWithEntityToday);

        assertEquals(0L, result);
    }
    @Test
    private void checkWeeklyNoMatch() {

        //all test trade data are for 2018 so this will cause no matches
        TotalVolumeTradedByLegalEntity extractor = new TotalVolumeTradedByLegalEntity();

        Map<RfqMetadataFieldNames, Object> meta = extractor.extractMetaData(rfq, session, trades);

        Object result = meta.get(RfqMetadataFieldNames.tradesWithEntityPastWeek);

        assertEquals(0L, result);
    }

    @Test
    private void checkYearlyNoMatch() {

        //all test trade data are for 2018 so this will cause no matches
        TotalVolumeTradedByLegalEntity extractor = new TotalVolumeTradedByLegalEntity();
        extractor.setDate(37, 1, 2);

        Map<RfqMetadataFieldNames, Object> meta = extractor.extractMetaData(rfq, session, trades);

        Object result = meta.get(RfqMetadataFieldNames.tradesWithEntityPastYear);

        assertEquals(0L, result);
    }


    @Test
    private void checkDailyMatch() {

        //all test trade data are for 2018 so this will cause no matches
        TotalVolumeTradedByLegalEntity extractor = new TotalVolumeTradedByLegalEntity();
        extractor.setDate(1460, 4, 178);

        Map<RfqMetadataFieldNames, Object> meta = extractor.extractMetaData(rfq, session, trades);

        Object result = meta.get(RfqMetadataFieldNames.tradesWithEntityToday);

        assertEquals(1350000L, result);
    }
    @Test
    private void checkWeeklyMatch() {

        //all test trade data are for 2018 so this will cause no matches
        TotalVolumeTradedByLegalEntity extractor = new TotalVolumeTradedByLegalEntity();
        extractor.setDate(37, 4, 178);

        Map<RfqMetadataFieldNames, Object> meta = extractor.extractMetaData(rfq, session, trades);

        Object result = meta.get(RfqMetadataFieldNames.tradesWithEntityPastWeek);

        assertEquals(1350000L, result);
    }

    @Test
    private void checkYearlyMatch() {

        //all test trade data are for 2018 so this will cause no matches
        TotalVolumeTradedByLegalEntity extractor = new TotalVolumeTradedByLegalEntity();
        extractor.setDate(37, 4, 178);

        Map<RfqMetadataFieldNames, Object> meta = extractor.extractMetaData(rfq, session, trades);

        Object result = meta.get(RfqMetadataFieldNames.tradesWithEntityPastYear);

        assertEquals(1350000L, result);
    }

}
