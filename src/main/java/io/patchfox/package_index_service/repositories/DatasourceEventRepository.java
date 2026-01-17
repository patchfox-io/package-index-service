package io.patchfox.package_index_service.repositories;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import io.patchfox.db_entities.entities.DatasourceEvent;

public interface DatasourceEventRepository extends JpaRepository<DatasourceEvent, Long> {


    @Modifying
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    @Query(
        value = "UPDATE datasource_event " +
	            "SET package_index_enriched = true, status = 'READY_FOR_NEXT_PROCESSING' " +
	            "WHERE id = ?1 ",
        nativeQuery = true
    )
    void setStatusFlagsFor(Long id);

 }
