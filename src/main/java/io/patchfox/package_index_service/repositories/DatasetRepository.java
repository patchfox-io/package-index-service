package io.patchfox.package_index_service.repositories;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import io.patchfox.db_entities.entities.Dataset;

public interface DatasetRepository extends JpaRepository<Dataset, Long> {
    public List<Dataset> findByName(String name);
}
