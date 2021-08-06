package com.dlwlrma.flink.api.demo;

import java.io.Serializable;

public class BehaviorInfo implements Serializable {
    private String id;
    private String userId;
    private String behavior;
    private String agentId;
    private long time;

    public BehaviorInfo(String userId, String agentId, String behavior) {
        this.userId = userId;
        this.agentId = agentId;
        this.behavior = behavior;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getBehavior() {
        return behavior;
    }

    public void setBehavior(String behavior) {
        this.behavior = behavior;
    }

    public long getTime() {
        return time;
    }

    public void setTime(long time) {
        this.time = time;
    }

    public String getAgentId() {
        return agentId;
    }

    public void setAgentId(String agentId) {
        this.agentId = agentId;
    }

    @Override
    public String toString() {
        return "BehaviorInfo{" +
                "id='" + id + '\'' +
                ", userId='" + userId + '\'' +
                ", behavior='" + behavior + '\'' +
                ", agentId='" + agentId + '\'' +
                ", time=" + time +
                '}';
    }
}