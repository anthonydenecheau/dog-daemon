package com.scc.daemon.services;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.sleuth.Span;
import org.springframework.cloud.sleuth.Tracer;
import org.springframework.stereotype.Service;

import com.scc.daemon.model.Breeder;
import com.scc.daemon.model.Dog;
import com.scc.daemon.model.Owner;
import com.scc.daemon.model.Parent;
import com.scc.daemon.model.Pedigree;
import com.scc.daemon.model.SyncData;
import com.scc.daemon.model.Title;
import com.scc.daemon.repository.BreederRepository;
import com.scc.daemon.repository.DogRepository;
import com.scc.daemon.repository.SyncDataRepository;
import com.scc.daemon.repository.OwnerRepository;
import com.scc.daemon.repository.ParentRepository;
import com.scc.daemon.repository.PedigreeRepository;
import com.scc.daemon.repository.TitleRepository;
import com.scc.events.source.SimpleSourceBean;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Service
public class DogService {

   private static final Logger logger = LoggerFactory.getLogger(DogService.class);

   @Autowired
   private Tracer tracer;

   @Autowired
   private SimpleSourceBean simpleSourceBean;

   @Autowired
   private ParentRepository parentRepository;

   @Autowired
   private PedigreeRepository pedigreeRepository;

   @Autowired
   private TitleRepository titleRepository;

   @Autowired
   private OwnerRepository ownerRepository;

   @Autowired
   private BreederRepository breederRepository;

   @Autowired
   private DogRepository dogRepository;

   @Autowired
   private SyncDataRepository syncDataRepository;

   public List<SyncData> getAllDogs() {
      //        Span newSpan = tracer.createSpan("getAllDogs");
      //        logger.debug("In the dogService.getAllDogs() call, trace id: {}", tracer.getCurrentSpan().traceIdString());

      try {
         return syncDataRepository.findByTransfertAndDomaine("N", "CHIEN");
      } finally {
         //            newSpan.tag("peer.service", "dogscheduler");
         //            newSpan.logEvent(org.springframework.cloud.sleuth.Span.CLIENT_RECV);
         //            tracer.close(newSpan);        	
      }

   }

   public List<SyncData> getAllBreeders() {
      //        Span newSpan = tracer.createSpan("getAllBreeders");
      //        logger.debug("In the dogService.getAllBreeders() call, trace id: {}", tracer.getCurrentSpan().traceIdString());

      try {
         return syncDataRepository.findByTransfertAndDomaine("N", "ELEVEUR");
      } finally {
         //            newSpan.tag("peer.service", "dogscheduler");
         //            newSpan.logEvent(org.springframework.cloud.sleuth.Span.CLIENT_RECV);
         //            tracer.close(newSpan);        	
      }

   }

   public List<SyncData> getAllOwners() {
      //        Span newSpan = tracer.createSpan("getAllOwners");
      //        logger.debug("In the dogService.getAllOwners() call, trace id: {}", tracer.getCurrentSpan().traceIdString());

      try {
         return syncDataRepository.findByTransfertAndDomaine("N", "PROPRIETAIRE");
      } finally {
         //            newSpan.tag("peer.service", "dogscheduler");
         //            newSpan.logEvent(org.springframework.cloud.sleuth.Span.CLIENT_RECV);
         //            tracer.close(newSpan);        	
      }

   }

   public List<SyncData> getAllTitles() {
      //        Span newSpan = tracer.createSpan("getAllTitles");
      //        logger.debug("In the dogService.getAllTitles() call, trace id: {}", tracer.getCurrentSpan().traceIdString());
      List<SyncData> _titles = new ArrayList<SyncData>();

      try {
         _titles = syncDataRepository.findByTransfertAndDomaine("N", "TITRE_FRANCAIS");
         _titles.addAll(syncDataRepository.findByTransfertAndDomaine("N", "TITRE_ETRANGER"));
      } finally {
         //            newSpan.tag("peer.service", "dogscheduler");
         //            newSpan.logEvent(org.springframework.cloud.sleuth.Span.CLIENT_RECV);
         //            tracer.close(newSpan);        	
      }

      return _titles;

   }

   public List<SyncData> getAllPedigree() {
      //        Span newSpan = tracer.createSpan("getAllPedigree");
      //        logger.debug("In the dogService.getAllPedigree() call, trace id: {}", tracer.getCurrentSpan().traceIdString());

      try {
         return syncDataRepository.findByTransfertAndDomaine("N", "LIVRE");
      } finally {
         //            newSpan.tag("peer.service", "dogscheduler");
         //            newSpan.logEvent(org.springframework.cloud.sleuth.Span.CLIENT_RECV);
         //            tracer.close(newSpan);        	
      }

   }

   public List<SyncData> getAllParent() {
      //        Span newSpan = tracer.createSpan("getAllParent");
      //        logger.debug("In the dogService.getAllParent() call, trace id: {}", tracer.getCurrentSpan().traceIdString());

      try {
         return syncDataRepository.findByTransfertAndDomaine("N", "GENITEUR");
      } finally {
         //            newSpan.tag("peer.service", "dogscheduler");
         //            newSpan.logEvent(org.springframework.cloud.sleuth.Span.CLIENT_RECV);
         //            tracer.close(newSpan);        	
      }

   }

   public Dog getDogById(long dogId) {
      Span newSpan = tracer.createSpan("getDogById");
      logger.debug("In the dogService.getDogById() call, trace id: {}", tracer.getCurrentSpan().traceIdString());

      try {
         return dogRepository.findById(dogId);
      } finally {
         newSpan.tag("peer.service", "dogscheduler");
         newSpan.logEvent(org.springframework.cloud.sleuth.Span.CLIENT_RECV);
         tracer.close(newSpan);
      }
   }

   public SyncData saveDog(SyncData dog) {
      Span newSpan = tracer.createSpan("saveDog");
      logger.debug("In the dogService.saveDog() call, trace id: {}", tracer.getCurrentSpan().traceIdString());

      try {
         return syncDataRepository.save(dog);
      } finally {
         newSpan.tag("peer.service", "dogscheduler");
         newSpan.logEvent(org.springframework.cloud.sleuth.Span.CLIENT_RECV);
         tracer.close(newSpan);
      }
   }

   public void refreshDog(Dog dog, String action) {
      Span newSpan = tracer.createSpan("publishDogChange");
      logger.debug("In the dogService.refreshDog() call, trace id: {}", tracer.getCurrentSpan().traceIdString());

      try {

         if (dog != null) {
            switch (action) {
            case "U":
               simpleSourceBean.publishDogChange("UPDATE", dog);
               break;
            case "I":
               simpleSourceBean.publishDogChange("SAVE", dog);
               break;
            case "D":
               simpleSourceBean.publishDogChange("DELETE", dog);
               break;
            default:
               logger.error("Received an UNKNOWN event from the dog service of type {}", action);
               break;
            }
         }
      } finally {
         newSpan.tag("peer.service", "dogscheduler");
         newSpan.logEvent(org.springframework.cloud.sleuth.Span.CLIENT_RECV);
         tracer.close(newSpan);
      }

   }

   public void refreshBreeder(Breeder dog, String action) {
      Span newSpan = tracer.createSpan("publishBreederChange");
      logger.debug("In the dogService.refreshBreeder() call, trace id: {}", tracer.getCurrentSpan().traceIdString());

      try {

         if (dog != null) {

            List<Breeder> breeders = new ArrayList<Breeder>();
            breeders.add(dog);

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
               logger.error("Received an UNKNOWN event from the dog breeder service of type {}", action);
               break;
            }
         }
      } finally {
         newSpan.tag("peer.service", "dogscheduler");
         newSpan.logEvent(org.springframework.cloud.sleuth.Span.CLIENT_RECV);
         tracer.close(newSpan);
      }

   }

   public Breeder getBreederByIdDog(int dogId) {
      Span newSpan = tracer.createSpan("getBreederByIdDog");
      logger.debug("In the dogService.getBreederByIdDog() call, trace id: {}", tracer.getCurrentSpan().traceIdString());

      try {
         return breederRepository.findByIdDog(dogId);
      } finally {
         newSpan.tag("peer.service", "dogscheduler");
         newSpan.logEvent(org.springframework.cloud.sleuth.Span.CLIENT_RECV);
         tracer.close(newSpan);
      }
   }

   public void refreshOwner(Owner dog, String action) {
      Span newSpan = tracer.createSpan("publishOwnerChange");
      logger.debug("In the dogService.refreshOwner() call, trace id: {}", tracer.getCurrentSpan().traceIdString());

      try {

         if (dog != null) {

            List<Owner> owners = new ArrayList<Owner>();
            owners.add(dog);

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
               logger.error("Received an UNKNOWN event from the dog owner service of type {}", action);
               break;
            }
         }
      } finally {
         newSpan.tag("peer.service", "dogscheduler");
         newSpan.logEvent(org.springframework.cloud.sleuth.Span.CLIENT_RECV);
         tracer.close(newSpan);
      }

   }

   public Owner getOwnerByIdDog(int dogId) {
      Span newSpan = tracer.createSpan("getOwnerByIdDog");
      logger.debug("In the dogService.getOwnerByIdDog() call, trace id: {}", tracer.getCurrentSpan().traceIdString());

      try {
         return ownerRepository.findByIdDog(dogId);
      } finally {
         newSpan.tag("peer.service", "dogscheduler");
         newSpan.logEvent(org.springframework.cloud.sleuth.Span.CLIENT_RECV);
         tracer.close(newSpan);
      }
   }

   public void refreshTitle(Title title, String action) {
      Span newSpan = tracer.createSpan("publishTitleChange");
      logger.debug("In the dogService.refreshTitle() call, trace id: {}", tracer.getCurrentSpan().traceIdString());

      try {

         if (title != null) {
            switch (action) {
            case "U":
               simpleSourceBean.publishTitleChange("UPDATE", title);
               break;
            case "I":
               simpleSourceBean.publishTitleChange("SAVE", title);
               break;
            case "D":
               simpleSourceBean.publishTitleChange("DELETE", title);
               break;
            default:
               logger.error("Received an UNKNOWN event from the dog title service of type {}", action);
               break;
            }
         }
      } finally {
         newSpan.tag("peer.service", "dogscheduler");
         newSpan.logEvent(org.springframework.cloud.sleuth.Span.CLIENT_RECV);
         tracer.close(newSpan);
      }

   }

   public Title getTitleById(long id) {
      Span newSpan = tracer.createSpan("getTitleById");
      logger.debug("In the dogService.getTitleById() call, trace id: {}", tracer.getCurrentSpan().traceIdString());

      try {
         return titleRepository.findById(id);
      } finally {
         newSpan.tag("peer.service", "dogscheduler");
         newSpan.logEvent(org.springframework.cloud.sleuth.Span.CLIENT_RECV);
         tracer.close(newSpan);
      }
   }

   public void refreshPedigree(Pedigree pedigree, String action) {
      Span newSpan = tracer.createSpan("publishPedigreeChange");
      logger.debug("In the dogService.refreshPedigree() call, trace id: {}", tracer.getCurrentSpan().traceIdString());

      try {

         if (pedigree != null) {
            switch (action) {
            case "U":
               simpleSourceBean.publishPedigreeChange("UPDATE", pedigree);
               break;
            case "I":
               simpleSourceBean.publishPedigreeChange("SAVE", pedigree);
               break;
            case "D":
               simpleSourceBean.publishPedigreeChange("DELETE", pedigree);
               break;
            default:
               logger.error("Received an UNKNOWN event from the dog pedigree service of type {}", action);
               break;
            }
         }
      } finally {
         newSpan.tag("peer.service", "dogscheduler");
         newSpan.logEvent(org.springframework.cloud.sleuth.Span.CLIENT_RECV);
         tracer.close(newSpan);
      }

   }

   public Pedigree getPedigreeById(long id) {
      Span newSpan = tracer.createSpan("getPedigreeByIdDog");
      logger.debug("In the dogService.getPedigreeByIdDog() call, trace id: {}",
            tracer.getCurrentSpan().traceIdString());

      try {
         return pedigreeRepository.findById(id);
      } finally {
         newSpan.tag("peer.service", "dogscheduler");
         newSpan.logEvent(org.springframework.cloud.sleuth.Span.CLIENT_RECV);
         tracer.close(newSpan);
      }
   }

   public void refreshParent(Parent dog, String action) {
      Span newSpan = tracer.createSpan("publishParentChange");
      logger.debug("In the dogService.refreshParent() call, trace id: {}", tracer.getCurrentSpan().traceIdString());

      try {

         if (dog != null) {
            switch (action) {
            case "U":
               simpleSourceBean.publishParentChange("UPDATE", dog);
               break;
            case "I":
               simpleSourceBean.publishParentChange("SAVE", dog);
               break;
            case "D":
               simpleSourceBean.publishParentChange("DELETE", dog);
               break;
            default:
               logger.error("Received an UNKNOWN event from the dog pedigree service of type {}", action);
               break;
            }
         }
      } finally {
         newSpan.tag("peer.service", "dogscheduler");
         newSpan.logEvent(org.springframework.cloud.sleuth.Span.CLIENT_RECV);
         tracer.close(newSpan);
      }

   }

   public Parent getParentByIdDog(int dogId) {
      Span newSpan = tracer.createSpan("getParentByIdDog");
      logger.debug("In the dogService.getParentByIdDog() call, trace id: {}", tracer.getCurrentSpan().traceIdString());

      try {
         return parentRepository.findById(dogId);
      } finally {
         newSpan.tag("peer.service", "dogscheduler");
         newSpan.logEvent(org.springframework.cloud.sleuth.Span.CLIENT_RECV);
         tracer.close(newSpan);
      }
   }

   public void deleteDog(SyncData dog) {
      try {
         syncDataRepository.delete(dog);
      } finally {
      }
   }

   public void syncDogInfos() {

      List<SyncData> dogList = new ArrayList<SyncData>();
      long idDog = 0;

      try {

         dogList = getAllDogs();
         if (dogList.size() > 0) {

            Span newSpan = tracer.createSpan("dogscheduler");
            logger.debug("In the dogService.syncDogInfos() call, trace id: {}",
                  tracer.getCurrentSpan().traceIdString());

            logger.debug("syncDogInfos :: dogList {}", dogList.size());

            // [[Boucle]] s/ le chien
            for (SyncData syncDog : dogList) {

               try {

                  // 1. Maj du chien de la table (WS_DOG_SYNC_DATA)
                  idDog = syncDog.getId();
                  syncDog.setTransfert("O");
                  saveDog(syncDog);

                  // 2. Lecture des infos pour le chien à synchroniser 
                  // Note : vue WS_DOG (Oracle) == image de la table WS_DOG (PostGRE)
                  // Si UPDATE/INSERT et dog == null alors le chien n'est pas dans le périmètre -> on le supprime de la liste
                  // + DELETE, dog == null -> on publie uniquement l'id à supprimer
                  Dog dog = new Dog();
                  if (!syncDog.getAction().equals("D")) {
                     dog = getDogById(idDog);
                     if (dog == null) {
                        deleteDog(syncDog);
                        continue;
                     }
                  } else
                     dog.withId(idDog);

                  // 3. Envoi du message à dog-service pour maj Postgre
                  // Pour le chien lui-même, ses titres, ses livres, ses parents, son proprietaire ?	
                  refreshDog(dog, syncDog.getAction());

               } catch (Exception e) {
                  logger.error(" idDog {} : {}", idDog, e.getMessage());
               } finally {
               }
            }
            // [[Boucle]]

            newSpan.tag("peer.service", "dogscheduler");
            newSpan.logEvent(org.springframework.cloud.sleuth.Span.CLIENT_RECV);
            tracer.close(newSpan);

         }

      } finally {
         dogList.clear();
      }

   }

   public void syncBreederInfos() {

      List<SyncData> dogList = new ArrayList<SyncData>();
      int idDog = 0;

      try {

         dogList = getAllBreeders();
         if (dogList.size() > 0) {

            Span newSpan = tracer.createSpan("dogscheduler");
            logger.debug("In the dogService.syncBreederInfos() call, trace id: {}",
                  tracer.getCurrentSpan().traceIdString());

            logger.debug("syncBreederInfos :: dogList {}", dogList.size());

            // [[Boucle]] s/ le chien
            for (SyncData syncDog : dogList) {

               try {

                  // 1. Maj du chien de la table (WS_DOG_SYNC_DATA)
                  idDog = (int) syncDog.getId();
                  syncDog.setTransfert("O");
                  saveDog(syncDog);

                  // 2. Lecture des infos pour le chien à synchroniser 
                  // Note : vue WS_DOG_ELEVEUR (Oracle) == image de la table WS_DOG_ELEVEUR (PostGRE)
                  // Si UPDATE/INSERT et dog == null alors le chien n'est pas dans le périmètre -> on le supprime de la liste
                  // + DELETE, dog == null -> on publie uniquement l'id à supprimer
                  Breeder breeder = new Breeder();
                  if (!syncDog.getAction().equals("D")) {
                     breeder = getBreederByIdDog(idDog);
                     if (breeder == null) {
                        deleteDog(syncDog);
                        continue;
                     }
                  } else
                     breeder.withIdDog(idDog);

                  // 3. Envoi du message à dog-service pour maj Postgre
                  refreshBreeder(breeder, syncDog.getAction());

               } catch (Exception e) {
                  logger.error(" idDog {} : {}", idDog, e.getMessage());
               } finally {
               }
            }
            // [[Boucle]]

            newSpan.tag("peer.service", "dogscheduler");
            newSpan.logEvent(org.springframework.cloud.sleuth.Span.CLIENT_RECV);
            tracer.close(newSpan);

         }

      } finally {
         dogList.clear();
      }

   }

   public void syncOwnerInfos() {

      List<SyncData> dogList = new ArrayList<SyncData>();
      int idDog = 0;

      try {

         dogList = getAllOwners();
         if (dogList.size() > 0) {

            Span newSpan = tracer.createSpan("dogscheduler");
            logger.debug("In the dogService.syncOwnerInfos() call, trace id: {}",
                  tracer.getCurrentSpan().traceIdString());

            logger.debug("getAllOwners :: dogList {}", dogList.size());

            // [[Boucle]] s/ le chien
            for (SyncData syncDog : dogList) {

               try {

                  // 1. Maj du chien de la table (WS_DOG_SYNC_DATA)
                  idDog = (int) syncDog.getId();
                  syncDog.setTransfert("O");
                  saveDog(syncDog);

                  // 2. Lecture des infos pour le chien à synchroniser 
                  // Note : vue WS_DOG_PROPRIETAIRE (Oracle) == image de la table WS_DOG_PROPRIETAIRE (PostGRE)
                  // Si UPDATE/INSERT et dog == null alors le chien n'est pas dans le périmètre -> on le supprime de la liste
                  // + DELETE, dog == null -> on publie uniquement l'id à supprimer
                  Owner owner = new Owner();
                  if (!syncDog.getAction().equals("D")) {
                     owner = getOwnerByIdDog(idDog);
                     if (owner == null) {
                        deleteDog(syncDog);
                        continue;
                     }
                  } else
                     owner.withId(idDog);

                  // 3. Envoi du message à dog-service pour maj Postgre
                  refreshOwner(owner, syncDog.getAction());

               } catch (Exception e) {
                  logger.error(" idDog {} : {}", idDog, e.getMessage());
               } finally {
               }
            }
            // [[Boucle]]

            newSpan.tag("peer.service", "dogscheduler");
            newSpan.logEvent(org.springframework.cloud.sleuth.Span.CLIENT_RECV);
            tracer.close(newSpan);

         }

      } finally {
         dogList.clear();
      }

   }

   public void syncTitleInfos() {

      List<SyncData> titles = new ArrayList<SyncData>();
      long idTitle = 0;
      String country = "";

      try {

         titles = getAllTitles();
         if (titles.size() > 0) {

            Span newSpan = tracer.createSpan("dogscheduler");
            logger.debug("In the dogService.syncTitleInfos() call, trace id: {}",
                  tracer.getCurrentSpan().traceIdString());

            logger.debug("getAllTitles :: titles {}", titles.size());

            // [[Boucle]] s/ les titres
            for (SyncData syncTitle : titles) {

               try {

                  // 1. Maj du titre de la table (WS_DOG_SYNC_DATA)
                  idTitle = syncTitle.getId();
                  country = (syncTitle.getDomaine().equals("TITRE_FRANCAIS") ? "FR" : "ETR");
                  syncTitle.setTransfert("O");
                  saveDog(syncTitle);

                  // 2. Lecture des infos pour le chien à synchroniser 
                  // Note : vue WS_DOG_TITRES (Oracle) == image de la table WS_DOG_TITRES (PostGRE)
                  // Si UPDATE/INSERT et dog == null alors le chien n'est pas dans le périmètre -> on le supprime de la liste
                  // + DELETE, dog == null -> on publie uniquement l'id à supprimer
                  Title title = new Title();
                  if (!syncTitle.getAction().equals("D")) {
                     title = getTitleById(idTitle);
                     if (title == null) {
                        deleteDog(syncTitle);
                        continue;
                     }
                  } else
                     title.withId(idTitle);

                  // 3. Envoi du message à dog-service pour maj Postgre
                  refreshTitle(title, syncTitle.getAction());

               } catch (Exception e) {
                  logger.error(" idTitle {} : {}", idTitle, e.getMessage());
               } finally {
               }
            }
            // [[Boucle]]

            newSpan.tag("peer.service", "dogscheduler");
            newSpan.logEvent(org.springframework.cloud.sleuth.Span.CLIENT_RECV);
            tracer.close(newSpan);

         }

      } finally {
         titles.clear();
      }

   }

   public void syncPedigreeInfos() {

      List<SyncData> pedigrees = new ArrayList<SyncData>();
      long idPedigree = 0;

      try {

         pedigrees = getAllPedigree();
         if (pedigrees.size() > 0) {

            Span newSpan = tracer.createSpan("dogscheduler");
            logger.debug("In the dogService.syncPedigreeInfos() call, trace id: {}",
                  tracer.getCurrentSpan().traceIdString());

            logger.debug("getAllPedigree :: pedigrees {}", pedigrees.size());

            // [[Boucle]] s/ le chien
            for (SyncData syncPedigree : pedigrees) {

               try {

                  // 1. Maj du chien de la table (WS_DOG_SYNC_DATA)
                  idPedigree = syncPedigree.getId();
                  syncPedigree.setTransfert("O");
                  saveDog(syncPedigree);

                  // 2. Lecture des infos pour le chien à synchroniser 
                  // Note : vue WS_DOG_LIVRES (Oracle) == image de la table WS_DOG_LIVRES (PostGRE)
                  // Si UPDATE/INSERT et dog == null alors le chien n'est pas dans le périmètre -> on le supprime de la liste
                  // + DELETE, dog == null -> on publie uniquement l'id à supprimer
                  Pedigree pedigree = new Pedigree();
                  if (!syncPedigree.getAction().equals("D")) {
                     pedigree = getPedigreeById(idPedigree);
                     if (pedigree == null) {
                        deleteDog(syncPedigree);
                        continue;
                     }
                  } else
                     pedigree.withId(idPedigree);

                  // 3. Envoi du message à dog-service pour maj Postgre
                  refreshPedigree(pedigree, syncPedigree.getAction());

               } catch (Exception e) {
                  logger.error(" idPedigree {} : {}", idPedigree, e.getMessage());
               } finally {
               }
            }
            // [[Boucle]]

            newSpan.tag("peer.service", "dogscheduler");
            newSpan.logEvent(org.springframework.cloud.sleuth.Span.CLIENT_RECV);
            tracer.close(newSpan);

         }

      } finally {
         pedigrees.clear();
      }

   }

   public void syncParentInfos() {

      List<SyncData> dogList = new ArrayList<SyncData>();
      int idDog = 0;

      try {

         dogList = getAllParent();
         if (dogList.size() > 0) {

            Span newSpan = tracer.createSpan("dogscheduler");
            logger.debug("In the dogService.syncParentInfos() call, trace id: {}",
                  tracer.getCurrentSpan().traceIdString());

            logger.debug("getAllParent :: dogList {}", dogList.size());

            // [[Boucle]] s/ le chien
            for (SyncData syncDog : dogList) {

               try {

                  // 1. Maj du chien de la table (WS_DOG_SYNC_DATA)
                  idDog = (int) syncDog.getId();
                  syncDog.setTransfert("O");
                  saveDog(syncDog);

                  // 2. Lecture des infos pour le chien à synchroniser 
                  // Note : vue WS_DOG_GENITEURS (Oracle) == image de la table WS_DOG_GENITEURS (PostGRE)
                  // Si UPDATE/INSERT et dog == null alors le chien n'est pas dans le périmètre -> on le supprime de la liste
                  // + DELETE, dog == null -> on publie uniquement l'id à supprimer
                  Parent parent = new Parent();
                  if (!syncDog.getAction().equals("D")) {
                     parent = getParentByIdDog(idDog);
                     if (parent == null) {
                        deleteDog(syncDog);
                        continue;
                     }
                  } else
                     parent.withId(idDog);

                  // 3. Envoi du message à dog-service pour maj Postgre
                  refreshParent(parent, syncDog.getAction());

               } catch (Exception e) {
                  logger.error(" idDog {} : {}", idDog, e.getMessage());
               } finally {
               }
            }
            // [[Boucle]]

            newSpan.tag("peer.service", "dogscheduler");
            newSpan.logEvent(org.springframework.cloud.sleuth.Span.CLIENT_RECV);
            tracer.close(newSpan);

         }

      } finally {
         dogList.clear();
      }

   }

}
