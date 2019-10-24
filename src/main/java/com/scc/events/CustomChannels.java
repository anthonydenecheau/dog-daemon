package com.scc.events;

import org.springframework.cloud.stream.annotation.Output;
import org.springframework.messaging.SubscribableChannel;

public interface CustomChannels {

   @Output("outboundDogChanges")
   SubscribableChannel outputDog();

   @Output("outboundOwnerChanges")
   SubscribableChannel outputOwner();

   @Output("outboundBreederChanges")
   SubscribableChannel outputBreeder();

   @Output("outboundTitleChanges")
   SubscribableChannel outputTitle();

   @Output("outboundPedigreeChanges")
   SubscribableChannel outputPedigree();

   @Output("outboundParentChanges")
   SubscribableChannel outputParent();

}
