package com.cs.rfq.decorator.extractors;

import com.cs.rfq.decorator.TradeDataLoader;
import com.cs.rfq.decorator.Rfq;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import java.util.HashMap;
import java.util.Map;

public class InstrumentLiquidityTest extends  AbstractSparkUnitTest{
    private Rfq rfq;
    Dataset<Row> trades;

    @BeforeEach
    public void setup() {
        String filePath = "src/test/resources/trades/trades.json";
        trades = new TradeDataLoader().loadTrades(session, filePath);
        rfq = new Rfq();
        rfq.setEntityId(5561279226039690843L);
        rfq.setIsin("AT0000A0VRQ6");
    }


    @Test
    public void checkVolumeWhenAllTradesMatch() {

        InstrumentLiquidity extractor = new InstrumentLiquidity();

        Map<RfqMetadataFieldNames, Object> meta = extractor.extractMetaData(rfq, session, trades);

        Object result = meta.get(RfqMetadataFieldNames.instrumentLiquidity);

        assertEquals(1_350_000L, result);
    }
}
