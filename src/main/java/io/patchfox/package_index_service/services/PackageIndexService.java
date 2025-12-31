package io.patchfox.package_index_service.services;

import com.github.packageurl.MalformedPackageURLException;
import com.github.packageurl.PackageURL;
import io.patchfox.db_entities.entities.DatasourceEvent;
import io.patchfox.db_entities.entities.Package;
import io.patchfox.package_index_service.components.EnvironmentComponent;
import io.patchfox.package_index_service.helpers.RestHelper;
import io.patchfox.package_index_service.repositories.DatasourceEventRepository;
import io.patchfox.package_index_service.repositories.PackageRepository;
import io.patchfox.package_utils.json.ApiRequest;
import io.patchfox.package_utils.json.ApiResponse;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.catalina.connector.Response;
import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.http.HttpStatusCode;
import org.springframework.stereotype.Component;

import java.net.URI;
import java.net.URISyntaxException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.regex.Pattern;
import java.util.concurrent.ThreadLocalRandom;

@Component
@Slf4j
public class PackageIndexService {
    @Autowired
    EnvironmentComponent env;

    @Autowired
    PackageRepository packageRepository;

    @Autowired
    DatasourceEventRepository datasourceEventRepository;

    @Autowired
    RestHelper restHelper;

    public static final String SERVICE_VERSION = "@project.version@";
    public static final String SEMVER_REGEX = "^(0|[1-9]\\d*)\\.(0|[1-9]\\d*)\\.(0|[1-9]\\d*)"
            + "(?:-([0-9A-Za-z-]+(?:\\.[0-9A-Za-z-]+)*))?"
            + "(?:\\+([0-9A-Za-z-]+(?:\\.[0-9A-Za-z-]+)*))?$";
    public static final String MAVEN_PACKAGE_TYPE = "maven";
    public static final String NPM_PACKAGE_TYPE = "npm";
    public static final String PYPI_PACKAGE_TYPE = "pypi";
    public static final String RUBY_PACKAGE_TYPE = "gem";
    public static final String GOLANG_PACKAGE_TYPE = "golang";
    public static final String PHP_PACKAGE_TYPE = "composer";
    public static final String RUST_PACKAGE_TYPE = "cargo";
    public static final String DOTNET_PACKAGE_TYPE = "nuget";

    // rows=200 is max for that argument. any higher and it reverts to default of 20
    public static final String MAVEN_API_TEMPLATE = "https://search.maven.org/solrsearch/select?q=g:%s+AND+a:%s&rows=200&wt=json&core=gav";
    public static final String NPM_API_TEMPLATE = "https://registry.npmjs.org/%s";
    public static final String PYPI_API_TEMPLATE = "https://pypi.org/pypi/%s/json";
    public static final String RUBY_API_TEMPLATE = "https://rubygems.org/api/v1/versions/%s.json";
    public static final String GOLANG_API_TEMPLATE_VERSION_LIST = "https://proxy.golang.org/%s/%s/@v/list";
    public static final String GOLANG_API_TEMPLATE_VERSION_INFO = "https://proxy.golang.org/%s/%s/@v/%s.info";
    public static final String PHP_API_TEMPLATE = "https://repo.packagist.org/p2/%s/%s.json";
    public static final String RUST_API_TEMPLATE = "https://crates.io/api/v1/crates/%s";
    public static final String DOTNET_API_TEMPLATE = "https://api.nuget.org/v3/registration5-gz-semver2/%s/index.json";
    @ToString
    public static class VersionMetadata {
        public String version;
        public ZonedDateTime releaseTimestamp;

        public VersionMetadata(String version, ZonedDateTime releaseTimestamp) {
            this.version = version;
            this.releaseTimestamp = releaseTimestamp;
        }
    }

    public static class PackageMetadata {
        public VersionMetadata latestVersion;
        public List<VersionMetadata> packageHistory; // list of package versions in newest to oldest order
        
        public PackageMetadata(VersionMetadata latestVersion, List<VersionMetadata> packageHistory) {
            this.latestVersion = latestVersion;
            this.packageHistory = packageHistory;
        }
    }

    public static class IndexQueryContext {
        public final UUID txid;
        public final String packageNamespace;
        public final String packageName;
        public final URI queryURI;

        public IndexQueryContext(UUID txid, String packageNamespace, String packageName, URI queryURI) {
            this.txid = txid;
            this.packageNamespace = packageNamespace;
            this.packageName = packageName;
            this.queryURI = queryURI;
        }
    }

    interface PackageOperator<T> {
        ZonedDateTime constructTimestamp(T timestamp);
        PackageMetadata getPackageMetadata(ApiResponse apiResponse, IndexQueryContext... args) throws Exception;
    }

    class MavenOperator implements PackageOperator<Long> {
        @Override
        public ZonedDateTime constructTimestamp(Long timestamp) {
            return ZonedDateTime.ofInstant(
                    (timestamp > 100000000000L)
                            ? Instant.ofEpochMilli(timestamp)  // Likely milliseconds
                            : Instant.ofEpochSecond(timestamp), // Likely seconds
                    ZoneOffset.UTC
            );
        }

        @Override
        public PackageMetadata getPackageMetadata(ApiResponse apiResponse, IndexQueryContext... args) {
            var response = apiResponse.getData();
            log.debug("response is: {}", response);
            JSONObject responseData = new JSONObject(response.get("response").toString());
            JSONObject responseObj = responseData.getJSONObject("response");
            JSONArray metadataArray = responseObj.getJSONArray("docs");

            List<VersionMetadata> packageHistory = new ArrayList<>();
            for (int i = 0; i < metadataArray.length(); i ++) {
                JSONObject versionMetadata = metadataArray.getJSONObject(i);
                if (versionMetadata.has("v") && versionMetadata.has("timestamp")) {
                    String version = versionMetadata.getString("v");
                    ZonedDateTime releaseTimestamp = constructTimestamp(versionMetadata.getLong("timestamp"));
                    packageHistory.add(new VersionMetadata(version, releaseTimestamp));
                }
            }
            // Sort newest to oldest based on releaseTimestamp
            packageHistory.sort((a, b) -> b.releaseTimestamp.compareTo(a.releaseTimestamp));

            // If packageHistory is empty, there was likely an issue with the query to the index
            if (packageHistory.isEmpty()) {
                IndexQueryContext queryContext = args[0];
                String debugID = UUID.randomUUID().toString();
                log.info("{} | NO PACKAGE VERSION HISTORY RECEIVED MAVEN", debugID);
                log.info("{} | txid: {}", debugID, queryContext.txid);
                log.info("{} | queried endpoint: {}", debugID, queryContext.queryURI.toString());
                log.info("{} | packageNamespace: {}", debugID, queryContext.packageNamespace);
                log.info("{} | packageName: {}", debugID, queryContext.packageName);
                log.info("{} | response from endpoint: {}", debugID, response);
                throw new NoSuchElementException("No package version history received");
            }
            return new PackageMetadata(packageHistory.get(0), packageHistory);
        }
    }

    class NpmOperator implements PackageOperator<String> {
        @Override
        public ZonedDateTime constructTimestamp(String timestamp) {
            // timestamp string is already in ISO 8601 UTC format
            return ZonedDateTime.parse(timestamp);
        }

        @Override
        public PackageMetadata getPackageMetadata(ApiResponse apiResponse, IndexQueryContext... args) {
            var response = apiResponse.getData();
            log.debug("response is: {}", response);
            JSONObject responseData = new JSONObject(response.get("response").toString());
            JSONObject versionDict = responseData.getJSONObject("time");

            // identify and trim all non-version keys from dictionary
            Pattern validVersionPattern = Pattern.compile(SEMVER_REGEX);
            List<String> extraKeys = new ArrayList<>();
            for (String key : versionDict.keySet()) {
                if (!validVersionPattern.matcher(key).matches()) {
                    extraKeys.add(key);
                }
            }

            for (String key : extraKeys) {
                versionDict.remove(key);
            }

            List<VersionMetadata> packageHistory = new ArrayList<>();
            for (String version : versionDict.keySet()) {
                ZonedDateTime releaseTimestamp = constructTimestamp(versionDict.getString(version));
                packageHistory.add(new VersionMetadata(version, releaseTimestamp));
            }
            // Sort newest to oldest based on releaseTimestamp
            packageHistory.sort((a, b) -> b.releaseTimestamp.compareTo(a.releaseTimestamp));

            // If packageHistory is empty, there was likely an issue with the query to the index
            if (packageHistory.isEmpty()) {
                IndexQueryContext queryContext = args[0];
                String debugID = UUID.randomUUID().toString();
                log.info("{} | NO PACKAGE VERSION HISTORY RECEIVED NPM", debugID);
                log.info("{} | txid: {}", debugID, queryContext.txid);
                log.info("{} | queried endpoint: {}", debugID, queryContext.queryURI.toString());
                log.info("{} | packageNamespace: {}", debugID, queryContext.packageNamespace);
                log.info("{} | packageName: {}", debugID, queryContext.packageName);
                log.info("{} | response from endpoint: {}", debugID, response);
                throw new NoSuchElementException("No package version history received");
            }
            return new PackageMetadata(packageHistory.get(0), packageHistory);
        }
    }

    class PypiOperator implements PackageOperator<String> {
        @Override
        public ZonedDateTime constructTimestamp(String timestamp) {
            // timestamp string is already in ISO 8601 UTC format
            return ZonedDateTime.parse(timestamp);
        }

        @Override
        public PackageMetadata getPackageMetadata(ApiResponse apiResponse, IndexQueryContext... args) {
            var response = apiResponse.getData();
            log.debug("response is: {}", response);
            JSONObject responseData = new JSONObject(response.get("response").toString());
            JSONObject versionDict = responseData.getJSONObject("releases");

            List<VersionMetadata> packageHistory = new ArrayList<>();
            for (String version : versionDict.keySet()) {
                JSONArray metadataArr = versionDict.getJSONArray(version);
                // skip any versions that do not have associated metadata info
                if (!metadataArr.isEmpty()) {
                    JSONObject versionMetadata = metadataArr.getJSONObject(0);
                    ZonedDateTime releaseTimestamp = constructTimestamp(versionMetadata.getString("upload_time_iso_8601"));
                    packageHistory.add(new VersionMetadata(version, releaseTimestamp));
                }
            }
            // Sort newest to oldest based on releaseTimestamp
            packageHistory.sort((a, b) -> b.releaseTimestamp.compareTo(a.releaseTimestamp));

            // If packageHistory is empty, there was likely an issue with the query to the index
            if (packageHistory.isEmpty()) {
                IndexQueryContext queryContext = args[0];
                String debugID = UUID.randomUUID().toString();
                log.info("{} | NO PACKAGE VERSION HISTORY RECEIVED PYPI", debugID);
                log.info("{} | txid: {}", debugID, queryContext.txid);
                log.info("{} | queried endpoint: {}", debugID, queryContext.queryURI.toString());
                log.info("{} | packageNamespace: {}", debugID, queryContext.packageNamespace);
                log.info("{} | packageName: {}", debugID, queryContext.packageName);
                log.info("{} | response from endpoint: {}", debugID, response);
                throw new NoSuchElementException("No package version history received");
            }
            return new PackageMetadata(packageHistory.get(0), packageHistory);
        }
    }

    class RubyGemOperator implements PackageOperator<String> {
        @Override
        public ZonedDateTime constructTimestamp(String timestamp) {
            // timestamp string is already in ISO 8601 UTC format
            return ZonedDateTime.parse(timestamp);
        }

        @Override
        public PackageMetadata getPackageMetadata(ApiResponse apiResponse, IndexQueryContext... args) {
            var response = apiResponse.getData();
            log.debug("response is: {}", response);
            JSONArray responseData = new JSONArray(response.get("response").toString());

            List<VersionMetadata> packageHistory = new ArrayList<>();
            for (int i = 0; i < responseData.length(); i++) {
                JSONObject versionMetadata = responseData.getJSONObject(i);
                if (versionMetadata.has("number") && versionMetadata.has("created_at")) {
                    String version = versionMetadata.getString("number");
                    ZonedDateTime releaseTimestamp = constructTimestamp(versionMetadata.getString("created_at"));
                    packageHistory.add(new VersionMetadata(version, releaseTimestamp));
                }
            }
            // Sort newest to oldest based on releaseTimestamp
            packageHistory.sort((a, b) -> b.releaseTimestamp.compareTo(a.releaseTimestamp));

            // If packageHistory is empty, there was likely an issue with the query to the index
            if (packageHistory.isEmpty()) {
                IndexQueryContext queryContext = args[0];
                String debugID = UUID.randomUUID().toString();
                log.info("{} | NO PACKAGE VERSION HISTORY RECEIVED RUBY", debugID);
                log.info("{} | txid: {}", debugID, queryContext.txid);
                log.info("{} | queried endpoint: {}", debugID, queryContext.queryURI.toString());
                log.info("{} | packageNamespace: {}", debugID, queryContext.packageNamespace);
                log.info("{} | packageName: {}", debugID, queryContext.packageName);
                log.info("{} | response from endpoint: {}", debugID, response);
                throw new NoSuchElementException("No package version history received");
            }
            return new PackageMetadata(packageHistory.get(0), packageHistory);
        }
    }

    class GolangOperator implements PackageOperator<String> {
        @Override
        public ZonedDateTime constructTimestamp(String timestamp) {
            // timestamp string is already in ISO 8601 UTC format
            return ZonedDateTime.parse(timestamp);
        }

        @Override
        public PackageMetadata getPackageMetadata(ApiResponse apiResponse, IndexQueryContext... args) throws Exception {
            IndexQueryContext queryContext = args[0];
            var response = apiResponse.getData();
            log.debug("response is: {}", response);
            // retrieve list of all versions from response
            String versionListString = response.get("response").toString();
            String[] versionList = versionListString.split("\\R");

            // retrieve individual version metadata for each version
            List<VersionMetadata> packageHistory = new ArrayList<>();
            for (int i = 0; i < versionList.length; i++) {
                String version = versionList[i];
                URI versionInfoURI = new URI(String.format(
                        GOLANG_API_TEMPLATE_VERSION_INFO,
                        queryContext.packageNamespace,
                        queryContext.packageName,
                        version
                ));
                ApiResponse versionInfoResponse = queryPackageIndex(
                        queryContext.txid,
                        versionInfoURI
                );

                var versionInfo = versionInfoResponse.getData();
                log.debug("versionInfo is: {}", versionInfo);
                JSONObject versionMetadata = new JSONObject(versionInfo.get("response").toString());
                ZonedDateTime releaseTimestamp = constructTimestamp(versionMetadata.getString("Time"));
                packageHistory.add(new VersionMetadata(version, releaseTimestamp));
            }
            // Sort newest to oldest based on releaseTimestamp
            packageHistory.sort((a, b) -> b.releaseTimestamp.compareTo(a.releaseTimestamp));

            // If packageHistory is empty, there was likely an issue with the query to the index
            if (packageHistory.isEmpty()) {
                String debugID = UUID.randomUUID().toString();
                log.info("{} | NO PACKAGE VERSION HISTORY RECEIVED GOLANG", debugID);
                log.info("{} | txid: {}", debugID, queryContext.txid);
                log.info("{} | queried endpoint: {}", debugID, queryContext.queryURI.toString());
                log.info("{} | packageNamespace: {}", debugID, queryContext.packageNamespace);
                log.info("{} | packageName: {}", debugID, queryContext.packageName);
                log.info("{} | response from endpoint: {}", debugID, response);
                throw new NoSuchElementException("No package version history received");
            }
            return new PackageMetadata(packageHistory.get(0), packageHistory);
        }
    }

    class PHPOperator implements PackageOperator<String> {
        @Override
        public ZonedDateTime constructTimestamp(String timestamp) {
            // timestamp string is already in ISO 8601 UTC format
            return ZonedDateTime.parse(timestamp);
        }

        @Override
        public PackageMetadata getPackageMetadata(ApiResponse apiResponse, IndexQueryContext... args) {
            IndexQueryContext queryContext = args[0];
            var response = apiResponse.getData();
            log.debug("response is: {}", response);
            JSONObject responseData = new JSONObject(response.get("response").toString());
            JSONObject packageObject = responseData.getJSONObject("packages");
            JSONArray versionList = packageObject.getJSONArray(queryContext.packageNamespace + "/" + queryContext.packageName);

            List<VersionMetadata> packageHistory = new ArrayList<>();
            for (int i = 0; i < versionList.length(); i++) {
                JSONObject versionMetadata = versionList.getJSONObject(i);
                String version = versionMetadata.getString("version");
                ZonedDateTime releaseTimestamp = constructTimestamp(versionMetadata.getString("time"));
                packageHistory.add(new VersionMetadata(version, releaseTimestamp));
            }
            // Sort newest to oldest based on releaseTimestamp
            packageHistory.sort((a, b) -> b.releaseTimestamp.compareTo(a.releaseTimestamp));

            // If packageHistory is empty, there was likely an issue with the query to the index
            if (packageHistory.isEmpty()) {
                String debugID = UUID.randomUUID().toString();
                log.info("{} | NO PACKAGE VERSION HISTORY RECEIVED PHP", debugID);
                log.info("{} | txid: {}", debugID, queryContext.txid);
                log.info("{} | queried endpoint: {}", debugID, queryContext.queryURI.toString());
                log.info("{} | packageNamespace: {}", debugID, queryContext.packageNamespace);
                log.info("{} | packageName: {}", debugID, queryContext.packageName);
                log.info("{} | response from endpoint: {}", debugID, response);
                throw new NoSuchElementException("No package version history received");
            }
            return new PackageMetadata(packageHistory.get(0), packageHistory);
        }
    }

    class RustOperator implements PackageOperator<String> {
        @Override
        public ZonedDateTime constructTimestamp(String timestamp) {
            // timestamp string is already in ISO 8601 UTC format
            return ZonedDateTime.parse(timestamp);
        }

        @Override
        public PackageMetadata getPackageMetadata(ApiResponse apiResponse, IndexQueryContext... args) {
            var response = apiResponse.getData();
            log.debug("response is: {}", response);
            JSONObject responseData = new JSONObject(response.get("response").toString());
            JSONArray versionList = responseData.getJSONArray("versions");

            List<VersionMetadata> packageHistory = new ArrayList<>();
            for (int i = 0; i < versionList.length(); i++) {
                JSONObject versionMetadata = versionList.getJSONObject(i);
                if (versionMetadata.has("num") && versionMetadata.has("created_at")) {
                    String version = versionMetadata.getString("num");
                    ZonedDateTime releaseTimestamp = constructTimestamp(versionMetadata.getString("created_at"));
                    packageHistory.add(new VersionMetadata(version, releaseTimestamp));
                }
            }
            // Sort newest to oldest based on releaseTimestamp
            packageHistory.sort((a, b) -> b.releaseTimestamp.compareTo(a.releaseTimestamp));

            // If packageHistory is empty, there was likely an issue with the query to the index
            if (packageHistory.isEmpty()) {
                IndexQueryContext queryContext = args[0];
                String debugID = UUID.randomUUID().toString();
                log.info("{} | NO PACKAGE VERSION HISTORY RECEIVED RUST", debugID);
                log.info("{} | txid: {}", debugID, queryContext.txid);
                log.info("{} | queried endpoint: {}", debugID, queryContext.queryURI.toString());
                log.info("{} | packageNamespace: {}", debugID, queryContext.packageNamespace);
                log.info("{} | packageName: {}", debugID, queryContext.packageName);
                log.info("{} | response from endpoint: {}", debugID, response);
                throw new NoSuchElementException("No package version history received");
            }
            return new PackageMetadata(packageHistory.get(0), packageHistory);
        }
    }

    class DotnetOperator implements PackageOperator<String> {
        @Override
        public ZonedDateTime constructTimestamp(String timestamp) {
            // timestamp string is already in ISO 8601 UTC format
            return ZonedDateTime.parse(timestamp);
        }

        @Override
        public PackageMetadata getPackageMetadata(ApiResponse apiResponse, IndexQueryContext... args) throws Exception {
            var response = apiResponse.getData();
            log.debug("response is: {}", response);
            JSONObject responseData = new JSONObject(response.get("response").toString());
            JSONArray versionDictBlocks = responseData.getJSONArray("items");

            // iterate over partitioned list of versions
            List<VersionMetadata> packageHistory = new ArrayList<>();
            for (int i = 0; i < versionDictBlocks.length(); i++) {
                JSONObject versionDictBlock = versionDictBlocks.getJSONObject(i);
                JSONArray versionDictList = versionDictBlock.getJSONArray("items");
                // iterate through version dictionaries within the selected block
                for (int j = 0; j < versionDictList.length(); j++) {
                    JSONObject versionDict = versionDictList.getJSONObject(j);
                    JSONObject versionMetadata = versionDict.getJSONObject("catalogEntry");
                    String version = versionMetadata.getString("version");
                    ZonedDateTime releaseTimestamp = constructTimestamp(versionMetadata.getString("published"));
                    packageHistory.add(new VersionMetadata(version, releaseTimestamp));
                }
            }
            // Sort newest to oldest based on releaseTimestamp
            packageHistory.sort((a, b) -> b.releaseTimestamp.compareTo(a.releaseTimestamp));

            // If packageHistory is empty, there was likely an issue with the query to the index
            if (packageHistory.isEmpty()) {
                IndexQueryContext queryContext = args[0];
                String debugID = UUID.randomUUID().toString();
                log.info("{} | NO PACKAGE VERSION HISTORY RECEIVED DOTNET", debugID);
                log.info("{} | txid: {}", debugID, queryContext.txid);
                log.info("{} | queried endpoint: {}", debugID, queryContext.queryURI.toString());
                log.info("{} | packageNamespace: {}", debugID, queryContext.packageNamespace);
                log.info("{} | packageName: {}", debugID, queryContext.packageName);
                log.info("{} | response from endpoint: {}", debugID, response);
                throw new NoSuchElementException("No package version history received");
            }
            return new PackageMetadata(packageHistory.get(0), packageHistory);
        }
    }

    public ApiResponse enrichRecord(
            UUID txid, 
            ZonedDateTime requestReceivedAt, 
            DatasourceEvent datasourceEventRecord
    ) throws Exception {

        // construct initial response JSON object
        JSONObject response = new JSONObject();

        boolean httpCreatedCodeFlag = false;
        var desPackagePurlStrings = packageRepository.getPackagesByDatasourceEventId(datasourceEventRecord.getId());
        log.info("desPackagePurlStrings for dse id: {} is: {}", datasourceEventRecord.getId(), desPackagePurlStrings);
        // because PackageURL throws an exception and it is crunch time 
        var desPackagePurls = new ArrayList<PackageURL>();
        for (var purlString : desPackagePurlStrings) { desPackagePurls.add(new PackageURL(purlString)); }
        for (var packagePurl : desPackagePurls) {
            String packageType = packagePurl.getType();
            String packageNamespace = packagePurl.getNamespace();
            String packageName = packagePurl.getName();
            ApiResponse apiResponse = null;
            PackageMetadata metadata = null;
            IndexQueryContext queryContext = null;
            URI queryURI = null;
            PackageOperator operator;
    
            switch (packageType) {
                case MAVEN_PACKAGE_TYPE:
                    queryURI = new URI(String.format(MAVEN_API_TEMPLATE, packageNamespace, packageName));
                    apiResponse = queryPackageIndex(txid, queryURI);
                    if ( !HttpStatusCode.valueOf(apiResponse.getCode()).is2xxSuccessful() ) {
                        log.warn("request to Maven Central resulted in error code: {}", apiResponse.getCode());
                        continue;
                    }
                    queryContext = new IndexQueryContext(txid, packageNamespace, packageName, queryURI);
                    operator = new MavenOperator();
                    metadata = operator.getPackageMetadata(apiResponse, queryContext);
                    break;
                case NPM_PACKAGE_TYPE:
                    queryURI = new URI(String.format(NPM_API_TEMPLATE, packageName));
                    apiResponse = queryPackageIndex(txid, queryURI);
                    if ( !HttpStatusCode.valueOf(apiResponse.getCode()).is2xxSuccessful() ) {
                        log.warn("request to npm registry resulted in error code: {}", apiResponse.getCode());
                        continue;
                    }
                    queryContext = new IndexQueryContext(txid, packageNamespace, packageName, queryURI);
                    operator = new NpmOperator();
                    metadata = operator.getPackageMetadata(apiResponse, queryContext);
                    break;
                case PYPI_PACKAGE_TYPE:
                    queryURI = new URI(String.format(PYPI_API_TEMPLATE, packageName));
                    apiResponse = queryPackageIndex(txid, queryURI);
                    if ( !HttpStatusCode.valueOf(apiResponse.getCode()).is2xxSuccessful() ) {
                        log.warn("request to pypi registry resulted in error code: {}", apiResponse.getCode());
                        continue;
                    }
                    queryContext = new IndexQueryContext(txid, packageNamespace, packageName, queryURI);
                    operator = new PypiOperator();
                    metadata = operator.getPackageMetadata(apiResponse, queryContext);
                    break;
                case RUBY_PACKAGE_TYPE:
                    queryURI = new URI(String.format(RUBY_API_TEMPLATE, packageName));
                    apiResponse = queryPackageIndex(txid, queryURI);
                    if ( !HttpStatusCode.valueOf(apiResponse.getCode()).is2xxSuccessful() ) {
                        log.warn("request to Ruby Gem registry resulted in error code: {}", apiResponse.getCode());
                        continue;
                    }
                    queryContext = new IndexQueryContext(txid, packageNamespace, packageName, queryURI);
                    operator = new RubyGemOperator();
                    metadata = operator.getPackageMetadata(apiResponse, queryContext);
                    break;
                case GOLANG_PACKAGE_TYPE:
                    queryURI = new URI(String.format(GOLANG_API_TEMPLATE_VERSION_LIST, packageNamespace, packageName));
                    apiResponse = queryPackageIndex(txid, queryURI);
                    if ( !HttpStatusCode.valueOf(apiResponse.getCode()).is2xxSuccessful() ) {
                        log.warn("request to Golang module proxy resulted in error code: {}", apiResponse.getCode());
                        continue;
                    }
                    queryContext = new IndexQueryContext(txid, packageNamespace, packageName, queryURI);
                    operator = new GolangOperator();
                    metadata = operator.getPackageMetadata(apiResponse, queryContext);
                    break;
                case PHP_PACKAGE_TYPE:
                    queryURI = new URI(String.format(PHP_API_TEMPLATE, packageNamespace, packageName));
                    apiResponse = queryPackageIndex(txid, queryURI);
                    if ( !HttpStatusCode.valueOf(apiResponse.getCode()).is2xxSuccessful() ) {
                        log.warn("request to PHP Composer registry resulted in error code: {}", apiResponse.getCode());
                        continue;
                    }
                    queryContext = new IndexQueryContext(txid, packageNamespace, packageName, queryURI);
                    operator = new PHPOperator();
                    metadata = operator.getPackageMetadata(apiResponse, queryContext);
                    break;
                case RUST_PACKAGE_TYPE:
                    queryURI = new URI(String.format(RUST_API_TEMPLATE, packageName));
                    apiResponse = queryPackageIndex(txid, queryURI);
                    if ( !HttpStatusCode.valueOf(apiResponse.getCode()).is2xxSuccessful() ) {
                        log.warn("request to Rust Crates registry resulted in error code: {}", apiResponse.getCode());
                        continue;
                    }
                    queryContext = new IndexQueryContext(txid, packageNamespace, packageName, queryURI);
                    operator = new RustOperator();
                    metadata = operator.getPackageMetadata(apiResponse, queryContext);
                    break;
                case DOTNET_PACKAGE_TYPE:
                    queryURI = new URI(String.format(DOTNET_API_TEMPLATE, packageName.toLowerCase()));
                    apiResponse = queryPackageIndex(txid, queryURI);
                    if ( !HttpStatusCode.valueOf(apiResponse.getCode()).is2xxSuccessful() ) {
                        log.warn("request to Golang module proxy resulted in error code: {}", apiResponse.getCode());
                        continue;
                    }
                    queryContext = new IndexQueryContext(txid, packageNamespace, packageName, queryURI);
                    operator = new DotnetOperator();
                    metadata = operator.getPackageMetadata(apiResponse, queryContext);
                    break;
                default:
                    log.warn("skipping packageType {} because it's not yet supported", packageType);
                    continue;
            }


            log.debug("metadata is: {}", metadata);
            log.debug("number of package versions tracked is: {}", metadata.packageHistory.size());
            if (metadata.packageHistory.isEmpty()) { continue; }
    
            // retrieve all instances of the package from the DB so we can update values
            List<Package> relevantPackageRecords = packageRepository.findByNamespaceAndName(packageNamespace, packageName);
            log.debug("relevantPackageRecords is: {}", relevantPackageRecords);
            boolean latestVersionFound = false;
            boolean skipped = false;
            String mostRecentVersion = metadata.latestVersion.version;
            ZonedDateTime mostRecentVersionPublishedAt = metadata.latestVersion.releaseTimestamp;
            log.debug("mostRecentVersion is: {}", mostRecentVersion);
            log.debug("mostRecentVersionPublishedAt: {}", mostRecentVersionPublishedAt);
            List<Long> updatedIds = new ArrayList<>();

            for (Package currPackageRecord : relevantPackageRecords) {
                log.debug("currPackageRecord: {}", currPackageRecord.getPurl());
                log.debug("mostRecentVersion: {}", mostRecentVersion);

                if (currPackageRecord.getVersion() == null || currPackageRecord.getVersion().isEmpty()) {
                    skipped = true;
                    continue;
                }

                String currPackageVersion = currPackageRecord.getVersion();
                ZonedDateTime currPackagePublishedAt = null;
                for (var e : metadata.packageHistory) {
                        if (e.version.equals(currPackageVersion)) {
                        currPackagePublishedAt = e.releaseTimestamp;
                    }
                }
                

                // check to see if we've successfully enriched this record within the last 24 hours.
                // if we have - no need to do it again until it's been at least a day
                var currentDateTime = ZonedDateTime.now(ZoneOffset.UTC);
                var recordUpdatedRecently = currentDateTime.minusDays(1).isBefore(currPackageRecord.getUpdatedAt());
                var recordPreviouslyPackageEnriched = !(currPackageRecord.getMostRecentVersion() == null);
                if (recordUpdatedRecently && recordPreviouslyPackageEnriched) { 
                    log.info("skipping record: {} because it's already been enriched recently", currPackageRecord.getPurl());
                    skipped = true;
                    continue; 
                }
    
                // check if a record for the most recent version of the package already exists in the table
                if (Objects.equals(currPackageVersion, mostRecentVersion)) { latestVersionFound = true; }

                // now that we've checked already to see if this record has been previously package enriched
                // we can set the mostRecentVersion value 
                currPackageRecord.setMostRecentVersion(mostRecentVersion);

                // set version differences in package entry
                log.info("Determining version differences for {}", currPackageRecord.getPurl());
                log.info("currPackageVersion: {}", currPackageVersion);
                log.info("mostRecentVersion: {}", mostRecentVersion);
                int[] versionDiffs = getVersionDifferences(currPackageVersion, mostRecentVersion, metadata.packageHistory);
                currPackageRecord.setNumberMajorVersionsBehindHead(versionDiffs[0]);
                currPackageRecord.setNumberMinorVersionsBehindHead(versionDiffs[1]);
                currPackageRecord.setNumberPatchVersionsBehindHead(versionDiffs[2]);

                var numberVersionsBehind = 0;
                for (int i = 0; i < metadata.packageHistory.size(); i ++) {
                    if (metadata.packageHistory.get(i).version.equals(currPackageRecord.getVersion())){
                        numberVersionsBehind = i;
                        break;
                    }
                }
                currPackageRecord.setNumberVersionsBehindHead(numberVersionsBehind);
    
                // update datetime columns
                currPackageRecord.setMostRecentVersionPublishedAt(mostRecentVersionPublishedAt);
                currPackageRecord.setThisVersionPublishedAt(currPackagePublishedAt);
                currPackageRecord.setUpdatedAt(currentDateTime);
    
                // save entry to package repository
                updatedIds.add(currPackageRecord.getId());
                currPackageRecord = packageRepository.save(currPackageRecord);
                log.info("updated packageRecord: {}", currPackageRecord.getPurl());
            }
    
            response.put("updatedRecordIds", updatedIds);
    
            // create a record for the latest version if it was not found in the package repository
            if ( !latestVersionFound && !skipped) {
                PackageURL purl = null;
                try {
                    purl = new PackageURL(packageType, packageNamespace, packageName, mostRecentVersion, null, null);
                } catch (MalformedPackageURLException e) {
                    log.error("caught unexpected purl parsing error for: {}", purl);

                    datasourceEventRecord.setStatus(DatasourceEvent.Status.PROCESSING_ERROR);
                    datasourceEventRepository.save(datasourceEventRecord);

                    return ApiResponse.builder()
                                      .code(Response.SC_INTERNAL_SERVER_ERROR)
                                      .txid(txid)
                                      .requestReceivedAt(requestReceivedAt.toString())
                                      .build();
                }
                log.info("making record for latest version discovered through package index enrichment: {}", purl);
                Package latestVersionRecord = Package.builder()
                                                     .purl(purl.toString())
                                                     .type(packageType)
                                                     .namespace(packageNamespace)
                                                     .name(packageName)
                                                     .version(mostRecentVersion)
                                                     .mostRecentVersion(mostRecentVersion)
                                                     .updatedAt(ZonedDateTime.now(ZoneOffset.UTC))
                                                     .thisVersionPublishedAt(mostRecentVersionPublishedAt)
                                                     .mostRecentVersionPublishedAt(mostRecentVersionPublishedAt)
                                                     .build();

                try {
                    latestVersionRecord = packageRepository.save(latestVersionRecord);
                    response.put("createdRecordId", latestVersionRecord.getId());
                    httpCreatedCodeFlag = true;
                } catch (DataIntegrityViolationException e) {
                    log.warn("caught unexpected exception indicating package record: {} already exists", purl);
                }

            }
        }

        log.info("seting status flags for datasourceEvent: {}", datasourceEventRecord.getPurl());
        datasourceEventRepository.setStatusFlagsFor(datasourceEventRecord.getId());
        // datasourceEventRecord.setPackageIndexEnriched(true);
        // datasourceEventRecord.setStatus(DatasourceEvent.Status.READY_FOR_NEXT_PROCESSING);
        // datasourceEventRepository.save(datasourceEventRecord);
        // datasourceEventRepository.flush();

        var code = httpCreatedCodeFlag ? Response.SC_CREATED : Response.SC_OK;
        return ApiResponse.builder()
                          .code(code)
                          .txid(txid)
                          .requestReceivedAt(requestReceivedAt.toString())
                          .data(Map.of("response", response.toString()))
                          .build();
    }

    public ApiResponse queryPackageIndex(UUID txid, URI uri) throws URISyntaxException, InterruptedException {
        var apiRequest = ApiRequest.builder()
                .headers(Map.of("User-Agent", "PatchfoxPackageIndex/" + SERVICE_VERSION))
                .uri(uri)
                .verb(ApiRequest.httpVerb.GET)
                .txid(txid)
                .build();

        var apiResponse = restHelper.makeRequest(apiRequest);

        // retry the request with jittered exponential backoff if we get throttled by the package index
        long baseDelay = 1000;
        int attempt = 1;
        while (apiResponse.getCode() == 429) {
            long backoff = (long) (baseDelay * Math.pow(2, attempt - 1));
            long jitter = ThreadLocalRandom.current().nextLong(backoff / 2, backoff);

            log.warn("Attempt {} to {} returned HTTP 429. Retrying in {} ms...", attempt, uri, jitter);
            Thread.sleep(jitter);

            apiResponse = restHelper.makeRequest(apiRequest);
            attempt++;
        }

        return apiResponse;
    }

    //TODO: need to add error handling for the weird edge cases that should never pop up in practice
    // (latest version somehow being older than provided version)
    private static int[] getVersionDifferences(String providedVersion, String latestVersion, List<VersionMetadata> packageHistory) {
        // initialize all version discrepancy counts to -1
        // if the function returns all counts as -1, this indicates the provided version was not found
        int patchVersionsBehind = -1;
        int minorVersionsBehind = -1;
        int majorVersionsBehind = -1;

        // strip leading "v" character for golang versions
        if (providedVersion.startsWith("v")) {
            providedVersion = providedVersion.substring(1);
        }
        if (latestVersion.startsWith("v")) {
            latestVersion = latestVersion.substring(1);
        }

        // determine patch version discrepancy
        for (int i = 0; i < packageHistory.size(); i++) {
            String compareVersion = packageHistory.get(i).version;
            if (compareVersion.startsWith("v")) {
                compareVersion = compareVersion.substring(1);
            }

            if (providedVersion.equals(compareVersion)) {
                patchVersionsBehind = i;
                break;
            }
        }

        // if patchVersionsBehind is non-negative, providedVersion exists and we can continue
        if (patchVersionsBehind != -1) {
            int[] latestVersionComponents = parseVersion(latestVersion);
            int[] providedVersionComponents = parseVersion(providedVersion);
            log.info("latestVersionComponents for mostRecentVersion: {}", Arrays.toString(latestVersionComponents));
            log.info("providedVersionComponents for currPackageVersion: {}", Arrays.toString(providedVersionComponents));
            // single out each component difference to make the upcoming comparison more legible
            majorVersionsBehind = latestVersionComponents[0] - providedVersionComponents[0];
            minorVersionsBehind = latestVersionComponents[1] - providedVersionComponents[1];
        }

        return new int[]{majorVersionsBehind, minorVersionsBehind, patchVersionsBehind};
    }

    private static int[] parseVersion(String version) {
        String[] parts = version.split("\\.");
        log.info("version regex split: {}", Arrays.toString(parts));
        int[] components = new int[3];

        // Ensure each version component exists. Default to 0 if component is missing
        try {
            components[0] = Integer.parseInt(parts[0]);
            components[1] = (parts.length > 1) ? Integer.parseInt(parts[1]) : 0;
            components[2] = (parts.length > 2) ? Integer.parseInt(parts[2]) : 0;
        } catch (NumberFormatException nfe) {
            // happens when one or more components are non-numeric
            for (int i = 0; i < 3; i ++) { components[i] = 0; }
        }
        return components;
    }
}
