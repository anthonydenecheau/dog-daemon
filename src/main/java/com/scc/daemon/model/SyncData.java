package com.scc.daemon.model;

import java.io.Serializable;
import java.util.Date;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.IdClass;
import javax.persistence.Table;

@Entity
@Table(name = "ws_dog_sync_data")
@IdClass(SyncDataId.class)
public class SyncData implements Serializable {

	private static final long serialVersionUID = 1L;

	@Id
	@Column(name = "id", nullable = false)
	private long id;

	@Id
	@Column(name = "domaine", nullable = false)
	private String domaine;

	@Id
	@Column(name = "action", nullable = false)
	private String action;
	
	@Column(name = "on_transfert")
	private String transfert;
	
	@Column(name = "date_creation")
	private Date dateCreation;

	public long getId() { return id; }
	public void setId(long id) { this.id = id; }

	public String getDomaine() {return domaine;}
	public void setDomaine(String domaine) {this.domaine = domaine;}

	public String getAction() { return action; }
	public void setAction(String action) { this.action = action; }

	public String getTransfert() { return transfert; }
	public void setTransfert(String transfert) { this.transfert = transfert; }

	public Date getDateCreation() { return dateCreation; }
	public void setDateCreation(Date dateCreation) { this.dateCreation = dateCreation; }

}
