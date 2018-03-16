package com.scc.daemon.repository;

import java.util.List;

import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

import com.scc.daemon.model.Breeder;

@Repository
public interface BreederRepository extends CrudRepository<Breeder,String>  {
    public List<Breeder> findById(int id);
    public Breeder findByIdDog(int idDog);
    
}
