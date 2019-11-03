package com.scc.events.models;

import com.scc.daemon.model.Pedigree;

public class PedigreeChangeModel {

   private String type;
   private String action;
   private Pedigree pedigree;
   private String traceId;
   private long timestamp;

   public PedigreeChangeModel(String type, String action, Pedigree pedigree, String traceId, long timestamp) {
      super();
      this.type = type;
      this.action = action;
      this.pedigree = pedigree;
      this.traceId = traceId;
      this.timestamp = timestamp;
   }

   public String getType() {
      return type;
   }

   public void setType(String type) {
      this.type = type;
   }

   public String getAction() {
      return action;
   }

   public void setAction(String action) {
      this.action = action;
   }

   public Pedigree getPedigree() {
      return pedigree;
   }

   public void setPedigree(Pedigree pedigree) {
      this.pedigree = pedigree;
   }

   public String getTraceId() {
      return traceId;
   }

   public void setTraceId(String traceId) {
      this.traceId = traceId;
   }

   public long getTimestamp() {
      return timestamp;
   }

   public void setTimestamp(long timestamp) {
      this.timestamp = timestamp;
   }

}
