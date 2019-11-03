package com.scc.daemon.model;

import java.io.Serializable;

import javax.persistence.Embeddable;

@Embeddable
public class SyncDataId implements Serializable {

   private static final long serialVersionUID = 1L;
   private long id;
   private String domaine;
   private String action;

   public long getId() {
      return id;
   }

   public void setId(long id) {
      this.id = id;
   }

   public String getDomaine() {
      return domaine;
   }

   public void setDomaine(String domaine) {
      this.domaine = domaine;
   }

   public String getAction() {
      return action;
   }

   public void setAction(String action) {
      this.action = action;
   }

}
