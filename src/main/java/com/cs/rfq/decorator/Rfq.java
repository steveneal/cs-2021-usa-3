package com.cs.rfq.decorator;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.io.Serializable;
import java.lang.reflect.Type;
import java.util.Map;
import java.lang.Double;
import org.json.*;

public class Rfq implements Serializable {
    private String id;
    private String isin;
    private Long traderId;
    private Long entityId;
    private Long quantity;
    private Double price;
    private String side;

    public static Rfq fromJson(String json) {
        System.out.println(json);
        //TODO: build a new RFQ setting all fields from data passed in the RFQ json message
        JSONObject obj = new JSONObject(json);

        String id = obj.getString("id");
        String isin = obj.getString("instrumentId");
        long traderId = obj.getLong("traderId");
        long entityId = obj.getLong("entityId");
        long quantity = obj.getLong("qty");
        Double price = obj.getDouble("price");
        String side = obj.getString("side");



        return new Rfq(id, isin, traderId, entityId, quantity, price, side);
    }
    public Rfq(String id, String isin, Long traderId, Long entityId, Long quantity, Double price, String side) {
        this.id = id;
        this.isin = isin;
        this.traderId = traderId;
        this.entityId = entityId;
        this.quantity = quantity;
        this.price = price;
        this.side = side;
    }

    public Rfq(){

    }

    @Override
    public String toString() {
        return "Rfq{" +
                "id='" + id + '\'' +
                ", isin='" + isin + '\'' +
                ", traderId=" + traderId +
                ", entityId=" + entityId +
                ", quantity=" + quantity +
                ", price=" + price +
                ", side=" + side +
                '}';
    }

    public boolean isBuySide() {
        return "B".equals(side);
    }

    public boolean isSellSide() {
        return "S".equals(side);
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getIsin() {
        return isin;
    }

    public void setIsin(String isin) {
        this.isin = isin;
    }

    public Long getTraderId() {
        return traderId;
    }

    public void setTraderId(Long traderId) {
        this.traderId = traderId;
    }

    public Long getEntityId() {
        return entityId;
    }

    public void setEntityId(Long entityId) {
        this.entityId = entityId;
    }

    public Long getQuantity() {
        return quantity;
    }

    public void setQuantity(Long quantity) {
        this.quantity = quantity;
    }

    public Double getPrice() {
        return price;
    }

    public void setPrice(Double price) {
        this.price = price;
    }

    public String getSide() {
        return side;
    }

    public void setSide(String side) {
        this.side = side;
    }
}
