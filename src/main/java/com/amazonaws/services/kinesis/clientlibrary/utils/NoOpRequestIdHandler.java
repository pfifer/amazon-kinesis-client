/*
 *  Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *  Licensed under the Amazon Software License (the "License").
 *  You may not use this file except in compliance with the License.
 *  A copy of the License is located at
 *
 *  http://aws.amazon.com/asl/
 *
 *  or in the "license" file accompanying this file. This file is distributed
 *  on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *  express or implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 */
package com.amazonaws.services.kinesis.clientlibrary.utils;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.AmazonWebServiceResult;

public class NoOpRequestIdHandler implements RequestIdHandler {

    public static RequestIdHandler INSTANCE = new NoOpRequestIdHandler();

    @Override
    public void logRequestId(AmazonWebServiceResult<?> result) {
        //
        // Does nothing
        //
    }

    @Override
    public void logFailureRequestId(AmazonServiceException ase, AmazonWebServiceRequest request) {
        //
        // Does nothing
        //
    }
}
