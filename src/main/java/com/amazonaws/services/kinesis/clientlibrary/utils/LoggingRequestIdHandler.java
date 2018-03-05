/*
 * Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved. Licensed under the Amazon Software License
 * (the "License"). You may not use this file except in compliance with the License. A copy of the License is located at
 * http://aws.amazon.com/asl/ or in the "license" file accompanying this file. This file is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific
 * language governing permissions and limitations under the License.
 */
package com.amazonaws.services.kinesis.clientlibrary.utils;

import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.AmazonWebServiceResult;

import lombok.extern.apachecommons.CommonsLog;

@CommonsLog
public class LoggingRequestIdHandler implements RequestIdHandler {

    public static RequestIdHandler INSTANCE = new LoggingRequestIdHandler();

    private static final Pattern RESULT_METHOD_EXTRACTOR = Pattern.compile(".*\\.(.*)Result$");
    private static final Pattern REQUEST_METHOD_EXTRACTOR = Pattern.compile(".*\\.(.*)Request$");
    private static final Pattern SERVICE_NAME_EXTRACTOR = Pattern.compile(".*\\.([^.]+)\\.model\\..*");

    @Override
    public void logRequestId(AmazonWebServiceResult<?> result) {
        Optional<String> requestId = getRequestId(result);
        Optional<String> requestId2 = getRequestId2(result);
        String service = extractPattern(result, SERVICE_NAME_EXTRACTOR);
        String method = extractPattern(result, RESULT_METHOD_EXTRACTOR);
        log.info(formatLogMessage("Succeeded", requestId, requestId2, service, method));
    }

    public void logFailureRequestId(AmazonServiceException ase, AmazonWebServiceRequest request) {
        Optional<String> requestId = Optional.of(ase.getRequestId());
        Optional<String> requestId2 = RequestIdExtractor.getRequestId2(ase.getHttpHeaders());

        String service = extractPattern(request, SERVICE_NAME_EXTRACTOR);
        String method = extractPattern(request, REQUEST_METHOD_EXTRACTOR);

        log.info(formatLogMessage("Failed", requestId, requestId2, service, method));
    }

    private String formatLogMessage(String outcome, Optional<String> requestId, Optional<String> requestId2,
            String service, String method) {
        return String.format("%s Call %s/%s -- x-amzn-requestid: %s -- x-amz-id-2: %s", outcome, service, method,
                requestId.orElse("missing"), requestId2.orElse("missing"));
    }

    private String extractPattern(Object result, Pattern pattern) {
        Matcher matcher = pattern.matcher(result.getClass().getName());
        if (matcher.matches()) {
            String guessedName = matcher.group(1);
            if (guessedName == null || guessedName.isEmpty()) {
                return result.getClass().getName();
            }
            return guessedName;
        }
        return result.getClass().getName();
    }

}
