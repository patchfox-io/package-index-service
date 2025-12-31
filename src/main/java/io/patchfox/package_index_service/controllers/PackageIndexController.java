package io.patchfox.package_index_service.controllers;

import com.github.packageurl.MalformedPackageURLException;
import io.patchfox.package_index_service.repositories.DatasourceEventRepository;
import io.patchfox.package_index_service.services.PackageIndexService;
import io.patchfox.package_utils.json.ApiResponse;
import lombok.extern.slf4j.Slf4j;
import org.apache.catalina.connector.Response;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestAttribute;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.net.URISyntaxException;
import java.time.ZonedDateTime;
import java.util.UUID;

@Slf4j
@RestController
public class PackageIndexController {

    public static final String API_PATH_PREFIX = "/api/v1";
    public static final String ENRICH_PACKAGES_PATH = API_PATH_PREFIX + "/enrichPackages";
    public static final String POST_ENRICH_PACKAGES_SIGNATURE = "POST_" + ENRICH_PACKAGES_PATH;

    @Autowired
    PackageIndexService packageIndexService;

    @Autowired
    DatasourceEventRepository datasourceEventRepository;

    @PostMapping(
            value = ENRICH_PACKAGES_PATH,
            produces = MediaType.APPLICATION_JSON_VALUE
    )
    ResponseEntity<ApiResponse> packageIndexServiceHandler(
            @RequestAttribute UUID txid,
            @RequestAttribute ZonedDateTime requestReceivedAt,
            @RequestParam Long datasourceEventRecordId
    ) throws URISyntaxException, MalformedPackageURLException {
        var datasourceEventRecordOptional = datasourceEventRepository.findById(datasourceEventRecordId);

        if (datasourceEventRecordOptional.isEmpty()) {
            var apiResponse = ApiResponse.builder()
                                         .txid(txid)
                                         .requestReceivedAt(requestReceivedAt)
                                         .code(Response.SC_BAD_REQUEST)
                                         .serverMessage("datasourceEvent record does not exist")
                                         .build();

            return ResponseEntity.status(apiResponse.getCode()).body(apiResponse);
        }

        var datasourceEventRecord = datasourceEventRecordOptional.get();
        
        // TODO don't know why this doesn't work but it doesn't and we got a deadline so ... 
        //Hibernate.initialize(datasourceEventRecord.getPackages());

        ApiResponse apiResponse = null;
        try {
            apiResponse = packageIndexService.enrichRecord(txid, requestReceivedAt, datasourceEventRecord);
        } catch (Exception e) {
            log.error("unexpected error gathering package metadata from index: {}", e.toString());
        }
        return ResponseEntity.status(apiResponse.getCode()).body(apiResponse);
    }
}

