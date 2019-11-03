package com.scc.daemon.repository;

import java.util.List;

import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

import com.scc.daemon.model.Owner;

@Repository
public interface OwnerRepository extends CrudRepository<Owner, String> {
   public List<Owner> findById(int id);

   public Owner findByIdDog(int idDog);
}
