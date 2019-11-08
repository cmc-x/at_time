package com.doit.pv_uv.pojo;

public class EventUserCountBean {
    private String uid;
    private String eventName;
    private String time;
    private String eventTypeId;
    private String eventTypeName;
    private String province;
    private int count;

    public EventUserCountBean() {
    }

    public EventUserCountBean(String uid, String eventName, String time, String eventTypeId, String province) {
        this.uid = uid;
        this.eventName = eventName;
        this.time = time;
        this.eventTypeId = eventTypeId;
        this.province = province;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public String getUid() {
        return uid;
    }

    public void setUid(String uid) {
        this.uid = uid;
    }

    public String getEventName() {
        return eventName;
    }

    public void setEventName(String eventName) {
        this.eventName = eventName;
    }

    public String getTime() {
        return time;
    }

    public void setTime(String time) {
        this.time = time;
    }

    public String getEventTypeId() {
        return eventTypeId;
    }

    public void setEventTypeId(String eventTypeId) {
        this.eventTypeId = eventTypeId;
    }

    public String getEventTypeName() {
        return eventTypeName;
    }

    public void setEventTypeName(String eventTypeName) {
        this.eventTypeName = eventTypeName;
    }

    public String getProvince() {
        return province;
    }

    public void setProvince(String province) {
        this.province = province;
    }

    @Override
    public String toString() {
        return "EventUserCountBean{" +
                "uid='" + uid + '\'' +
                ", eventName='" + eventName + '\'' +
                ", time='" + time + '\'' +
                ", eventTypeId='" + eventTypeId + '\'' +
                ", eventTypeName='" + eventTypeName + '\'' +
                ", province='" + province + '\'' +
                ", count=" + count +
                '}';
    }
}
