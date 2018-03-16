package com.scc.daemon.repository;

import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

import com.scc.daemon.model.Dog;

@Repository
public interface DogRepository extends CrudRepository<Dog,String>  {
    public Dog findById(long id);
}
