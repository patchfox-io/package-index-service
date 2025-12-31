package io.patchfox.package_index_service.interceptors;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.patchfox.package_utils.json.ApiRequest;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.servlet.HandlerInterceptor;

import java.io.BufferedReader;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.UUID;


/*
 * Ensures all requests to this service have a unique transaction id and a received_at timestamp associated with them.
 * The intention is this information to be generated here, at point of entry into the pipeline, and attached to every 
 * stage of the PatchFox pipeline.  
 */
@Slf4j
public class RequestEnrichmentInterceptor implements HandlerInterceptor {
    
    public static final String EVENT_RECEIVED_AT_ATTRIBUTE = "requestReceivedAt";

    public static final String PACKAGE_RECORD_ID_KEY = "packageRecordId";
    public static final String PACKAGE_RECORD_ID_ATTRIBUTE = PACKAGE_RECORD_ID_KEY;
    public static final String PURL_LIST_ATTRIBUTE = "purlList";
    public static final String PURL_LIST_KEY = "purls";

    @Override
    public boolean preHandle(
        HttpServletRequest request,
        HttpServletResponse response, 
        Object handler
    ) throws Exception {
        var txid = UUID.randomUUID();
        // we want to use the caller supplied one if possible in order to keep the same txid value
        // accross the entire pipeline workflow
        if (request.getHeader(ApiRequest.TXID_KEY) != null) {
            try {
                txid = UUID.fromString(request.getHeader(ApiRequest.TXID_KEY));
            } catch (IllegalArgumentException e) {
                log.warn(
                    "caller supplied invalid txid: {} -- using newly generated one",
                    request.getHeader(ApiRequest.TXID_KEY)
                );
            }
        }

        /*-------- UNCOMMENT IF WE SUPPORT INCOMING PURL LIST
        // capture PURL list object
        StringBuilder stringBuilder = new StringBuilder();
        String line;
        try (BufferedReader reader = request.getReader()) {
            while ((line = reader.readLine()) != null) {
                stringBuilder.append(line);
            }
        }

        String requestBody = stringBuilder.toString();
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode requestBodyJson = objectMapper.readTree(requestBody);
        String[] purlList = requestBodyJson.get(PURL_LIST_KEY);

        if (purlList != null && purlList.isArray()) {
            String[] purls = new String[purlList.size()];
            for (int i = 0; i < purlList.size(); i++) {
                purls[i] = purlList.get(i).asText();
            }
            request.setAttribute(PURL_LIST_ATTRIBUTE, purls);
        } else {
            log.warn("No 'purls' array found in JSON.");
        }
        -------------*/

        // capture package DB record id
        String packageRecordIdString = request.getParameter(PACKAGE_RECORD_ID_KEY);
        if (packageRecordIdString != null) {
            try {
                // Convert the String to a long
                Long id = Long.parseLong(packageRecordIdString);
                request.setAttribute(PACKAGE_RECORD_ID_ATTRIBUTE, id);
            } catch (NumberFormatException e) {
                // Handle the case where the parameter is not a valid long
                log.warn("Invalid package record id provided. Not a valid long");
            }
        } else {
            log.warn("No package record id provided.");
        }

        request.setAttribute(ApiRequest.TXID_KEY, txid);
        var now = ZonedDateTime.ofInstant(Instant.now(), ZoneOffset.UTC);
        request.setAttribute(EVENT_RECEIVED_AT_ATTRIBUTE, now);
        return true;
    }

}
