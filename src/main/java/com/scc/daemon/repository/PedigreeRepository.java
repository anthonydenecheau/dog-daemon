package com.scc.daemon.repository;

import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

import com.scc.daemon.model.Pedigree;

@Repository
public interface PedigreeRepository extends CrudRepository<Pedigree, String> {
   public Pedigree findById(long id);
}
