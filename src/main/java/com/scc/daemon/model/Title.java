package com.scc.daemon.model;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

@Entity
@Table(name = "ws_dog_titres")
public class Title {

   @Id
   @Column(name = "id", nullable = false)
   private long id;

   @Column(name = "id_chien", nullable = false)
   private int idDog;

   @Column(name = "id_titre", nullable = false)
   private int idTitle;

   @Column(name = "code")
   private String title;

   @Column(name = "nom")
   private String name;

   @Column(name = "categorie")
   private String type;

   @Column(name = "pays")
   private String country;

   @Column(name = "date_obtention")
   private String obtentionDate;

   public long getId() {
      return id;
   }

   public void setId(long id) {
      this.id = id;
   }

   public int getIdDog() {
      return idDog;
   }

   public void setIdDog(int idDog) {
      this.idDog = idDog;
   }

   public int getIdTitle() {
      return idTitle;
   }

   public void setIdTitle(int idTitle) {
      this.idTitle = idTitle;
   }

   public String getTitle() {
      return title;
   }

   public void setTitle(String title) {
      this.title = title;
   }

   public String getName() {
      return name;
   }

   public void setName(String name) {
      this.name = name;
   }

   public String getType() {
      return type;
   }

   public void setType(String type) {
      this.type = type;
   }

   public String getCountry() {
      return country;
   }

   public void setCountry(String country) {
      this.country = country;
   }

   public String getObtentionDate() {
      return obtentionDate;
   }

   public void setObtentionDate(String obtentionDate) {
      this.obtentionDate = obtentionDate;
   }

   public Title withId(long id) {
      this.setId(id);
      return this;
   }

   public Title withIdDog(int idDog) {
      this.setIdDog(idDog);
      return this;
   }

   public Title withIdTitle(int idTitle) {
      this.setIdTitle(idTitle);
      return this;
   }

   public Title withObtentionDate(String obtentionDate) {
      this.setObtentionDate(obtentionDate);
      return this;
   }

   public Title withTitle(String title) {
      this.setTitle(title);
      return this;
   }

   public Title withName(String name) {
      this.setName(name);
      return this;
   }

   public Title withType(String type) {
      this.setType(type);
      return this;
   }

   public Title withCountry(String country) {
      this.setCountry(country);
      return this;
   }

}
