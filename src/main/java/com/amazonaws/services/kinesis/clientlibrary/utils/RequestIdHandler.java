/*
 * Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved. Licensed under the Amazon Software License
 * (the "License"). You may not use this file except in compliance with the License. A copy of the License is located at
 * http://aws.amazon.com/asl/ or in the "license" file accompanying this file. This file is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific
 * language governing permissions and limitations under the License.
 */
package com.amazonaws.services.kinesis.clientlibrary.utils;

import java.util.Optional;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.AmazonWebServiceResult;

public interface RequestIdHandler {

    void logRequestId(AmazonWebServiceResult<?> result);
    void logFailureRequestId(AmazonServiceException ase, AmazonWebServiceRequest request);

    default Optional<String> getRequestId(AmazonWebServiceResult<?> result) {
        if (result.getSdkResponseMetadata() != null) {
            return Optional.of(result.getSdkResponseMetadata().getRequestId());
        }
        return Optional.empty();
    }

    default Optional<String> getRequestId2(AmazonWebServiceResult<?> result) {
        if (result.getSdkHttpMetadata() != null && result.getSdkHttpMetadata().getHttpHeaders() != null) {
            return RequestIdExtractor.getRequestId2(result.getSdkHttpMetadata().getHttpHeaders());
        }
        return Optional.empty();
    }

}
