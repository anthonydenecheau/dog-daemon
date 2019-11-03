package com.scc.events.source;

import java.time.Instant;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.sleuth.Tracer;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.messaging.support.MessageBuilder;

import com.scc.daemon.model.Breeder;
import com.scc.daemon.model.Dog;
import com.scc.daemon.model.Owner;
import com.scc.daemon.model.Parent;
import com.scc.daemon.model.Pedigree;
import com.scc.daemon.model.Title;
import com.scc.events.CustomChannels;
import com.scc.events.models.BreederChangeModel;
import com.scc.events.models.DogChangeModel;
import com.scc.events.models.OwnerChangeModel;
import com.scc.events.models.ParentChangeModel;
import com.scc.events.models.PedigreeChangeModel;
import com.scc.events.models.TitleChangeModel;

@EnableBinding(CustomChannels.class)
public class SimpleSourceBean {

   private static final Logger logger = LoggerFactory.getLogger(SimpleSourceBean.class);

   @Autowired
   private Tracer tracer;

   @Autowired
   private CustomChannels customChannels;

   @SendTo("outboundBreederChanges")
   public void publishBreederChange(String action, List<Breeder> breeders) {

      Instant instant = Instant.now();
      try {

         for (Breeder breeder : breeders) {
            logger.debug("Sending Kafka message {} for Breeder Id: {} at {} ", action, breeder.getId(), instant);

            BreederChangeModel change = new BreederChangeModel(BreederChangeModel.class.getTypeName(), action, breeder,
                  tracer.getCurrentSpan().traceIdString(), instant.toEpochMilli());

            customChannels.outputBreeder().send(MessageBuilder.withPayload(change).build());
         }
      } finally {
      }

   }

   @SendTo("outboundOwnerChanges")
   public void publishOwnerChange(String action, List<Owner> owners) {
      Instant instant = Instant.now();

      try {

         for (Owner owner : owners) {
            logger.debug("Sending Kafka message {} for Owner Id: {} at {} ", action, owner.getId(), instant);

            OwnerChangeModel change = new OwnerChangeModel(OwnerChangeModel.class.getTypeName(), action, owner,
                  tracer.getCurrentSpan().traceIdString(), instant.toEpochMilli());

            customChannels.outputOwner().send(MessageBuilder.withPayload(change).build());
         }
      } finally {
      }

   }

   @SendTo("outboundDogChanges")
   public void publishDogChange(String action, Dog dog) {
      Instant instant = Instant.now();
      logger.debug("Sending Kafka message {} for Dog Id: {} at {} ", action, dog.getId(), instant);

      try {

         DogChangeModel change = new DogChangeModel(DogChangeModel.class.getTypeName(), action, dog,
               tracer.getCurrentSpan().traceIdString(), instant.toEpochMilli());

         customChannels.outputDog().send(MessageBuilder.withPayload(change).build());

      } finally {
      }

   }

   @SendTo("outboundTitleChanges")
   public void publishTitleChange(String action, Title title) {
      Instant instant = Instant.now();

      try {

         logger.debug("Sending Kafka message {} for Title Id: {} at {} ", action, title.getId(), instant);

         TitleChangeModel change = new TitleChangeModel(TitleChangeModel.class.getTypeName(), action, title,
               tracer.getCurrentSpan().traceIdString(), instant.toEpochMilli());

         customChannels.outputTitle().send(MessageBuilder.withPayload(change).build());

      } finally {
      }

   }

   @SendTo("outboundPedigreeChanges")
   public void publishPedigreeChange(String action, Pedigree pedigree) {
      Instant instant = Instant.now();

      try {

         logger.debug("Sending Kafka message {} for Pedigree Id: {} at {} ", action, pedigree.getId(), instant);

         PedigreeChangeModel change = new PedigreeChangeModel(PedigreeChangeModel.class.getTypeName(), action, pedigree,
               tracer.getCurrentSpan().traceIdString(), instant.toEpochMilli());

         customChannels.outputPedigree().send(MessageBuilder.withPayload(change).build());

      } finally {
      }

   }

   @SendTo("outboundParentChanges")
   public void publishParentChange(String action, Parent parent) {
      Instant instant = Instant.now();

      try {

         logger.debug("Sending Kafka message {} for Parent Id: {} at {} ", action, parent.getId(), instant);

         ParentChangeModel change = new ParentChangeModel(ParentChangeModel.class.getTypeName(), action, parent,
               tracer.getCurrentSpan().traceIdString(), instant.toEpochMilli());

         customChannels.outputParent().send(MessageBuilder.withPayload(change).build());

      } finally {
      }

   }
}
