package com.scc.tasks;

import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.sleuth.Span;
import org.springframework.cloud.sleuth.Tracer;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import com.scc.daemon.model.Breeder;
import com.scc.daemon.model.Owner;
import com.scc.daemon.model.SyncData;
import com.scc.daemon.services.PersonService;
import com.scc.daemon.services.DogService;

@Component
public class DogScheduler {
    
	private static final Logger logger = LoggerFactory.getLogger(DogScheduler.class);
	
	private static final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("HH:mm:ss");
	
    @Autowired
    private Tracer tracer;
    
	@Autowired
	DogService dogService;

	@Autowired
	PersonService personService;

	@Scheduled (fixedDelayString = "${fixedDelay.in.milliseconds}")
    public void synchronizeDog() {

		try {
        
	    	// I. Lecture dans la table demande de synchro pour l'ensemble des chiens sur lesquels 1 maj est demandée
	    	dogService.syncDogInfos();
	    	
	    	// II. Lecture dans la table demande de synchro pour l'ensemble des éleveurs sur lesquels 1 maj est demandée
	    	dogService.syncBreederInfos();
	    	
	    	// III. Lecture dans la table demande de synchro pour l'ensemble des propriétaires sur lesquels 1 maj est demandée
	    	dogService.syncOwnerInfos();
	    	
	    	// IV. Lecture dans la table demande de synchro pour l'ensemble des titres sur lesquels 1 maj est demandée
	    	dogService.syncTitleInfos();
	    	
	    	// V. Lecture dans la table demande de synchro pour l'ensemble des livres sur lesquels 1 maj est demandée
	    	dogService.syncPedigreeInfos();

	    	// VI. Lecture dans la table demande de synchro pour l'ensemble des géniteurs sur lesquels 1 maj est demandée
	    	dogService.syncParentInfos();
	    	
		} catch (Exception e) {
			logger.error(" synchronizeDog : {}", e.getMessage());
		} finally {
		}
    }

	@Scheduled (fixedDelayString = "${fixedDelay.in.milliseconds}")
    public void synchronizePerson() {

		try {
        
	    	//logger.debug("readDog :: Execution Time - {}", dateTimeFormatter.format(LocalDateTime.now()));
	    	List<SyncData> personList = new ArrayList<SyncData>();
	    	int idPerson = 0;
	    	
	    	// 0. Lecture dans la table demande de synchro pour l'ensemble des personnes (eleveurs et propriétaires) sur lesquelles 1 maj est demandée
	    	personList = personService.getAllPersons();
	    	
	    	if (personList.size() > 0 ) {

		    	logger.debug("getAllPersons :: personList {}", personList.size());

	    		Span newSpan = tracer.createSpan("dogscheduler");
	            logger.debug("In the dogScheduler.readPerson() call, trace id: {}", tracer.getCurrentSpan().traceIdString());

		    	// [[Boucle]] s/ la personne
		    	for (SyncData syncPers : personList) {
		    		
		            try {
		    				
		    	    	// 1. Maj du chien de la table (WS_DOG_SYNC_DATA)
		    			idPerson = (int) syncPers.getId();
		    			syncPers.setTransfert("O");
		    			personService.savePerson(syncPers);
		    			
		    	    	// 2. Lecture des infos pour l'éleveur/propriétaire à synchroniser 

			            // PARTIE 1. Info ELEVEUR
		    			// Note : vue WS_DOG_ELEVEUR (Oracle) == image de la table WS_DOG_ELEVEUR (PostGRE)
		    			// Si UPDATE/INSERT et breeder == null alors l'éleveur n'est pas dans le périmètre -> on le supprime de la liste
		    			// + DELETE, breeder == null -> on publie uniquement l'id à supprimer
		    			List<Breeder> breeders = new ArrayList<Breeder>();
		    			if (!syncPers.getAction().equals("D"))
		    				breeders = personService.getBreederById(idPerson);
		    			else
		    				breeders.add(new Breeder().withId(idPerson));
		    				
		    	    	// Envoi du message à dog-service pour maj Postgre
	    				if (breeders != null && breeders.size() > 0 )
	    					personService.refreshBreeder(breeders, syncPers.getAction());
			    			
			            // PARTIE 2. Info PROPRIETAIRE
		    			// Note : vue WS_DOG_PROPRIETAIRE (Oracle) == image de la table WS_DOG_PROPRIETAIRE (PostGRE)
		    			// Si UPDATE/INSERT et owner == null alors le propriétaire n'est pas dans le périmètre -> on le supprime de la liste
		    			// + DELETE, owner == null -> on publie uniquement l'id à supprimer
	    				List<Owner> owners = new ArrayList<Owner>();
		    			if (!syncPers.getAction().equals("D"))
		    				owners = personService.getOwnerById(idPerson);
		    			else
		    				owners.add(new Owner().withId(idPerson));
		    			
		    	    	// Envoi du message à dog-service pour maj Postgre
	    				if (owners != null && owners.size() > 0 )
	    					personService.refreshOwner(owners, syncPers.getAction());
	    				
	    				if ( (owners == null  && owners.size() == 0 ) && (breeders == null && breeders.size() == 0))
	    					personService.deletePerson(syncPers);
		    			
		    		} catch (Exception e) {
		    			logger.error(" idPerson {} : {}", idPerson, e.getMessage());
		    		} finally {
		    			newSpan.tag("peer.service", "dogscheduler");
		    			newSpan.logEvent(org.springframework.cloud.sleuth.Span.CLIENT_RECV);
		    			tracer.close(newSpan);		    			
		    		}

		    	}	    	
		    	// [[Boucle]]
	    	}
		
		} finally {
		}
    }

}
