package com.scc.events.models;

import com.scc.daemon.model.OdsOwner;

public class OdsOwnerChangeModel {

    private String type;
    private String action;
    private OdsOwner owner;
    private String traceId;
    private long timestamp;

    public  OdsOwnerChangeModel(String type, String action, OdsOwner owner, String traceId, long timestamp) {
        super();
        this.type   = type;
        this.action = action;
        this.owner = owner;
        this.traceId = traceId;
        this.timestamp = timestamp;
    }

	public String getType() { return type; }
    public void setType(String type) { this.type = type;}

    public String getAction() { return action;}
    public void setAction(String action) {this.action = action; }

    public OdsOwner getOwner() { return owner; }
    public void setOwner(OdsOwner owner) { this.owner = owner;}

    public String getTraceId() { return traceId; }
	public void setTraceId(String traceId) { this.traceId = traceId; }

	public long getTimestamp() { return timestamp; }
	public void setTimestamp(long timestamp) { this.timestamp = timestamp; }

}
