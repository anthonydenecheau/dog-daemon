package com.scc.daemon.repository;

import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

import com.scc.daemon.model.Parent;

@Repository
public interface ParentRepository extends CrudRepository<Parent, String> {
   public Parent findById(int id);
}
