package io.patchfox.package_index_service.repositories;

import org.springframework.data.jpa.repository.JpaRepository;
import io.patchfox.db_entities.entities.FindingData;

public interface FindingDataRepository extends JpaRepository<FindingData, Long>{}
