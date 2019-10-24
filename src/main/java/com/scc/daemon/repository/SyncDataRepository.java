package com.scc.daemon.repository;

import java.util.List;

import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

import com.scc.daemon.model.SyncData;

@Repository
public interface SyncDataRepository extends CrudRepository<SyncData, String> {
   public List<SyncData> findByTransfertAndDomaine(String transfert, String domaine);

   public SyncData save(SyncData dog);

   public void delete(SyncData dog);
}
