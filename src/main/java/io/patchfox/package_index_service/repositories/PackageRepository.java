package io.patchfox.package_index_service.repositories;

import io.patchfox.db_entities.entities.Package;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import com.github.packageurl.PackageURL;

import java.util.List;

public interface PackageRepository extends JpaRepository<Package, Long> {
    List<Package> findByNamespaceAndName(String namespace, String name);

    @Query(
        value = "SELECT p.purl " +
                "FROM package p " +
                "INNER JOIN datasource_event_package dep " +
                "ON dep.datasource_event_id  = :datasourceEventId " +
                "AND dep.package_id = p.id; ",
        nativeQuery = true
    )
    List<String> getPackagesByDatasourceEventId(@Param("datasourceEventId") long datasourceEventId);
}
