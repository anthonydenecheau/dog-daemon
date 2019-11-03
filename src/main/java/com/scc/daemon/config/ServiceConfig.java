package com.scc.daemon.config;

import java.sql.SQLException;

import javax.sql.DataSource;
import javax.validation.constraints.NotNull;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import oracle.jdbc.pool.OracleDataSource;

@Component
public class ServiceConfig {

   @NotNull
   @Value("${spring.datasource.username}")
   private String username;

   @NotNull
   @Value("${spring.datasource.password}")
   private String password;

   @NotNull
   @Value("${spring.datasource.url}")
   private String url;

   public void setUsername(String username) {
      this.username = username;
   }

   public void setPassword(String password) {
      this.password = password;
   }

   public void setUrl(String url) {
      this.url = url;
   }

   @Bean
   DataSource dataSource() throws SQLException {

      OracleDataSource dataSource = new OracleDataSource();
      dataSource.setUser(username);
      dataSource.setPassword(password);
      dataSource.setURL(url);
      dataSource.setImplicitCachingEnabled(true);
      dataSource.setFastConnectionFailoverEnabled(true);
      return dataSource;
   }
}
