package com.cs.rfq.decorator;

import com.cs.rfq.decorator.extractors.*;
import com.cs.rfq.decorator.publishers.MetadataJsonLogPublisher;
import com.cs.rfq.decorator.publishers.MetadataPublisher;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.spark.sql.functions.sum;

public class RfqProcessor {

    private final static Logger log = LoggerFactory.getLogger(RfqProcessor.class);

    private final SparkSession session;

    private final JavaStreamingContext streamingContext;

    private Dataset<Row> trades;

    private final List<RfqMetadataExtractor> extractors = new ArrayList<>();

    private final MetadataPublisher publisher = new MetadataJsonLogPublisher();

    public RfqProcessor(SparkSession session, JavaStreamingContext streamingContext) {
        this.session = session;
        this.streamingContext = streamingContext;

        //TODO: use the TradeDataLoader to load the trade data archives
        TradeDataLoader tdl = new TradeDataLoader();
        trades = tdl.loadTrades(session, "src\\test\\resources\\trades\\trades.json");
        //TODO: take a close look at how these two extractors are implemented
        //test test
        extractors.add(new TotalTradesWithEntityExtractor());
        extractors.add(new VolumeTradedWithEntityYTDExtractor());

        extractors.add(new AverageTradePriceWeekly());
        extractors.add(new InstrumentLiquidity());
        extractors.add(new TotalVolumeTradedByInstrument());
        extractors.add(new TotalVolumeTradedByLegalEntity());

    }

    public void startSocketListener() throws InterruptedException {
        //TODO: stream data from the input socket on localhost:9000
        JavaDStream<String> lines = streamingContext.socketTextStream("localhost", 9000);

        //TODO: convert each incoming line to a Rfq object and call processRfq method with it
        JavaDStream<Rfq> words = lines.map(x -> Rfq.fromJson(x));
        words.foreachRDD(rdd -> {
            rdd.collect().forEach(line -> processRfq(line));
        });
        //TODO: start the streaming context
        streamingContext.start();
        streamingContext.awaitTermination();
    }

    public void processRfq(Rfq rfq) {
        log.info(String.format("Received Rfq: %s", rfq.toString()));
        Map<RfqMetadataFieldNames, Object> metadata = new HashMap<RfqMetadataFieldNames, Object>();

        //TODO: get metadata from each of the extractors
        for(RfqMetadataExtractor extractor: extractors){
            metadata.putAll(extractor.extractMetaData(rfq, session, trades));
        }
        //TODO: publish the metadata
        publisher.publishMetadata(metadata);
        System.out.println(metadata);
    }
}
