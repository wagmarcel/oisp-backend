package com.oisp.databackend.datasources.tsdb;

import com.oisp.databackend.datastructures.Aggregator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@SuppressWarnings("PMD.TooManyMethods")
public class TsdbQuery {
    private String aid;
    private List<String> cid;
    private List<String> componentTypes;
    private List<String>  attributes;
    private List<Aggregator> aggregators;
    private List<String> orders;
    private boolean locationInfo;
    private long start;
    private long stop;
    private Long maxPoints;

    public TsdbQuery(String aid, String cid, List<String> attributes, boolean locationInfo, long start, long stop) {
        this.aid = aid;
        this.cid = Arrays.asList(cid);
        this.componentTypes = new ArrayList<>();
        this.aggregators = new ArrayList<>();
        this.orders = new ArrayList<>();
        this.attributes = attributes;
        this.locationInfo = locationInfo;
        this.start = start;
        this.stop = stop;
    }
    public TsdbQuery(String aid, List<String> cids, List<String> attributes, boolean locationInfo, long start, long stop) {
        this.aid = aid;
        this.cid = cids;
        this.componentTypes = new ArrayList<>();
        this.aggregators = new ArrayList<>();
        this.orders = new ArrayList<>();
        this.attributes = attributes;
        this.locationInfo = locationInfo;
        this.start = start;
        this.stop = stop;
    }

    public TsdbQuery() {
        this.locationInfo = false;
        this.componentTypes = new ArrayList<>();
        this.attributes = new ArrayList<>();
        this.cid = new ArrayList<>();
        this.aggregators = new ArrayList<>();
        this.orders = new ArrayList<>();
    }

    public TsdbQuery withAid(String aid) {
        this.aid = aid;
        return this;
    }

    public TsdbQuery withCid(String cid) {
        this.cid.add(cid);
        return this;
    }

    public TsdbQuery withCids(List<String> cids) {
        this.cid = cids;
        return this;
    }

    public TsdbQuery withComponentType(String componentType) {
        this.componentTypes.add(componentType);
        return this;
    }

    public TsdbQuery withComponentTypes(List<String> componentTypes) {
        this.componentTypes = componentTypes;
        return this;
    }

    public List<String> getComponentTypes() {
        return componentTypes;
    }

    public void setComponentTypes(List<String> componentTypes) {
        this.componentTypes = componentTypes;
    }

    public TsdbQuery withAttributes(List<String> attributes) {
        this.attributes = attributes;
        return this;
    }

    public TsdbQuery withAttributes(String[] attributes) {
        if (attributes == null) {
            this.attributes = new ArrayList<>();
            return this;
        }
        this.attributes = new ArrayList<String>(Arrays.asList(attributes));
        return this;
    }

    public TsdbQuery withLocationInfo(boolean locationInfo) {
        this.locationInfo = locationInfo;
        return this;
    }

    public TsdbQuery withStart(long start) {
        this.start = start;
        return this;
    }

    public TsdbQuery withStop(long stop) {
        this.stop = stop;
        return this;
    }

    public TsdbQuery withMaxPoints(Long maxPoints) {
        this.maxPoints = maxPoints;
        return this;
    }

    public Long getMaxPoints() {
        return maxPoints;
    }

    public void setMaxPoints(Long maxPoints) {
        this.maxPoints = maxPoints;
    }

    public void setAid(String aid) {
        this.aid = aid;
    }

    public void setCid(String cid) {
        this.cid.add(cid);
    }

    public void setAttributes(List<String> attributes) {
        this.attributes = attributes;
    }

    public void setLocationInfo(boolean locationInfo) {
        this.locationInfo = locationInfo;
    }

    public void setStart(long start) {
        this.start = start;
    }

    public void setStop(long stop) {
        this.stop = stop;
    }

    public String getAid() {

        return aid;
    }

    public String getCid() {
        return cid.get(0);
    }
    public List<String> getCids() {
        return cid;
    }

    public List<String> getAttributes() {
        return attributes;
    }

    public boolean isLocationInfo() {
        return locationInfo;
    }

    public long getStart() {
        return start;
    }

    public long getStop() {
        return stop;
    }

    public String getComponentType() {
        return componentTypes.get(0);
    }

    public void setComponentType(String componentType) {
        this.componentTypes.add(componentType);
    }

    public Aggregator getAggregator() {
        if (aggregators.size() != 0) {
            return aggregators.get(0);
        } else {
            return null;
        }
    }

    public void setAggregator(Aggregator aggregator) {
        if (this.aggregators.size() == 0) {
            this.aggregators.add(aggregator);
        } else {
            this.aggregators.set(0, aggregator);
        }
    }

    public TsdbQuery withAggregator(Aggregator aggregator) {
        setAggregator(aggregator);
        return this;
    }

    public String getOrder() {
        if (orders.size() != 0) {
            return orders.get(0);
        } else {
            return null;
        }
    }

    public void setOrder(String order) {
        if (this.orders.size() == 0) {
            this.orders.add(order);
        } else {
            this.orders.set(0, order);
        }

    }

    public TsdbQuery withOrder(String order) {
        setOrder(order);
        return this;
    }
}
