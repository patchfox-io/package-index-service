package io.patchfox.package_index_service.repositories;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;

import io.patchfox.db_entities.entities.Datasource;

public interface DatasourceRepository extends JpaRepository<Datasource, Long> {
    List<Datasource> findByPurl(String purl);
}
