/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.ml.common.optimizer.ps;

/** Message Type between worker and ps. */
public enum MessageType {
    PUSH_INTIALIZEd_MODEL((char) 0),
    PUSH_GRAD((char) 1),
    SPARSE_PULL_MODEL((char) 2),

    // ps to worker
    PULLED_MODEL((char) 3),
    // init as all zero.
    PSF_ZEROS((char) 4);

    public char type;

    MessageType(char type) {
        this.type = type;
    }

    public static MessageType valueOf(char value) {
        switch (value) {
            case (char) 0:
                return MessageType.PUSH_INTIALIZEd_MODEL;
            case (char) 1:
                return MessageType.PUSH_GRAD;
            case (char) 2:
                return MessageType.SPARSE_PULL_MODEL;
            case ((char) 3):
                return MessageType.PULLED_MODEL;
            case ((char) 4):
                return MessageType.PSF_ZEROS;
            default:
                throw new UnsupportedOperationException();
        }
    }
}
