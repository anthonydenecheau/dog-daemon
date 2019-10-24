package com.scc.daemon.services;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.sleuth.Span;
import org.springframework.cloud.sleuth.Tracer;
import org.springframework.stereotype.Service;

import com.scc.daemon.model.Breeder;
import com.scc.daemon.model.Owner;
import com.scc.daemon.model.SyncData;
import com.scc.daemon.repository.OwnerRepository;
import com.scc.daemon.repository.BreederRepository;
import com.scc.daemon.repository.SyncDataRepository;
import com.scc.events.source.SimpleSourceBean;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Service
public class PersonService {

   private static final Logger logger = LoggerFactory.getLogger(PersonService.class);

   @Autowired
   private Tracer tracer;

   @Autowired
   private SimpleSourceBean simpleSourceBean;

   @Autowired
   private SyncDataRepository syncDataRepository;

   @Autowired
   private BreederRepository breederRepository;

   @Autowired
   private OwnerRepository ownerRepository;

   public List<SyncData> getAllPersons() {
      //        Span newSpan = tracer.createSpan("getAllPersons");
      //        logger.debug("In the personService.getAllPersons() call, trace id: {}", tracer.getCurrentSpan().traceIdString());

      try {
         return syncDataRepository.findByTransfertAndDomaine("N", "PERSONNE");
      } finally {
         //            newSpan.tag("peer.service", "dogscheduler");
         //            newSpan.logEvent(org.springframework.cloud.sleuth.Span.CLIENT_RECV);
         //            tracer.close(newSpan);        	
      }

   }

   public SyncData savePerson(SyncData person) {
      Span newSpan = tracer.createSpan("savePerson");
      logger.debug("In the personService.savePerson() call, trace id: {}", tracer.getCurrentSpan().traceIdString());

      try {
         return syncDataRepository.save(person);
      } finally {
         newSpan.tag("peer.service", "dogscheduler");
         newSpan.logEvent(org.springframework.cloud.sleuth.Span.CLIENT_RECV);
         tracer.close(newSpan);
      }
   }

   public List<Breeder> getBreederById(int personId) {
      Span newSpan = tracer.createSpan("getBreederById");
      logger.debug("In the personService.getBreederById() call, trace id: {}", tracer.getCurrentSpan().traceIdString());

      try {
         return breederRepository.findById(personId);
      } finally {
         newSpan.tag("peer.service", "dogscheduler");
         newSpan.logEvent(org.springframework.cloud.sleuth.Span.CLIENT_RECV);
         tracer.close(newSpan);
      }
   }

   public void refreshBreeder(List<Breeder> breeders, String action) {
      Span newSpan = tracer.createSpan("publishBreederChange");
      logger.debug("In the personService.refreshBreeder() call, trace id: {}", tracer.getCurrentSpan().traceIdString());

      try {

         if (breeders != null && breeders.size() > 0) {
            switch (action) {
            case "U":
               simpleSourceBean.publishBreederChange("UPDATE", breeders);
               break;
            case "I":
               simpleSourceBean.publishBreederChange("SAVE", breeders);
               break;
            case "D":
               simpleSourceBean.publishBreederChange("DELETE", breeders);
               break;
            default:
               logger.error("Received an UNKNOWN event from the agria service of type {}", action);
               break;
            }
         }
      } finally {
         newSpan.tag("peer.service", "dogscheduler");
         newSpan.logEvent(org.springframework.cloud.sleuth.Span.CLIENT_RECV);
         tracer.close(newSpan);
      }

   }

   public List<Owner> getOwnerById(int personId) {
      Span newSpan = tracer.createSpan("getOwnerById");
      logger.debug("In the personService.getOwnerById() call, trace id: {}", tracer.getCurrentSpan().traceIdString());

      try {
         return ownerRepository.findById(personId);
      } finally {
         newSpan.tag("peer.service", "dogscheduler");
         newSpan.logEvent(org.springframework.cloud.sleuth.Span.CLIENT_RECV);
         tracer.close(newSpan);
      }
   }

   public void refreshOwner(List<Owner> owners, String action) {
      Span newSpan = tracer.createSpan("publishOwnerChange");
      logger.debug("In the personService.refreshOwner() call, trace id: {}", tracer.getCurrentSpan().traceIdString());

      try {

         if (owners != null && owners.size() > 0) {
            switch (action) {
            case "U":
               simpleSourceBean.publishOwnerChange("UPDATE", owners);
               break;
            case "I":
               simpleSourceBean.publishOwnerChange("SAVE", owners);
               break;
            case "D":
               simpleSourceBean.publishOwnerChange("DELETE", owners);
               break;
            default:
               logger.error("Received an UNKNOWN event from the dog person service of type {}", action);
               break;
            }
         }
      } finally {
         newSpan.tag("peer.service", "dogscheduler");
         newSpan.logEvent(org.springframework.cloud.sleuth.Span.CLIENT_RECV);
         tracer.close(newSpan);
      }

   }

   public void deletePerson(SyncData person) {
      try {
         syncDataRepository.delete(person);
      } finally {
      }
   }
}
