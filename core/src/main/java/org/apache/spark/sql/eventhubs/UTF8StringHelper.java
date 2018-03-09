/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.eventhubs;

import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.types.UTF8String;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.function.Function;

public class UTF8StringHelper {

    /**
     * Wrapper over `int` to allow result of parsing integer from string to be accessed via reference.
     * This is done solely for better performance and is not expected to be used by end users.
     */
    public static class IntWrapper implements Serializable {
        public transient int value = 0;
    }

    public static boolean toInt(UTF8String str, IntWrapper intWrapper) {

        boolean retVal = false;

        try {
            Field baseField = UTF8String.class.getDeclaredField("base");
            Field offsetField = UTF8String.class.getDeclaredField("offset");
            baseField.setAccessible(true);
            offsetField.setAccessible(true);

            Object _base = baseField.get(str);
            long _offset = offsetField.getLong(str);

            Function<Integer, Byte> getByte = (i) -> Platform.getByte(_base, _offset + i);

            int numBytes = str.numBytes();
            if (numBytes == 0) {
                retVal = false;
            }

            byte b = getByte.apply(0);
            final boolean negative = b == '-';
            int offset = 0;
            if (negative || b == '+') {
                offset++;
                if (numBytes == 1) {
                    retVal = false;
                }
            }

            final byte separator = '.';
            final int radix = 10;
            final int stopValue = Integer.MIN_VALUE / radix;
            int result = 0;

            while (offset < numBytes) {
                b = getByte.apply(offset);
                offset++;
                if (b == separator) {
                    // We allow decimals and will return a truncated integral in that case.
                    // Therefore we won't throw an exception here (checking the fractional
                    // part happens below.)
                    break;
                }

                int digit = 0;
                if (b >= '0' && b <= '9') {
                    digit = b - '0';
                } else {
                    retVal = false;
                }

                // We are going to process the new digit and accumulate the result. However, before doing
                // this, if the result is already smaller than the stopValue(Integer.MIN_VALUE / radix), then
                // result * 10 will definitely be smaller than minValue, and we can stop
                if (result < stopValue) {
                    retVal = false;
                }

                result = result * radix - digit;
                // Since the previous result is less than or equal to stopValue(Integer.MIN_VALUE / radix),
                // we can just use `result > 0` to check overflow. If result overflows, we should stop
                if (result > 0) {
                    retVal = false;
                }
            }

            // This is the case when we've encountered a decimal separator. The fractional
            // part will not change the number, but we will verify that the fractional part
            // is well formed.
            while (offset < numBytes) {
                byte currentByte = getByte.apply(offset);
                if (currentByte < '0' || currentByte > '9') {
                    retVal = false;
                }
                offset++;
            }

            if (!negative) {
                result = -result;
                if (result < 0) {
                    retVal = false;
                }
            }
            intWrapper.value = result;
            retVal = true;
        } catch (Exception e) {
            // Should never happen. Spark 2.2 and Spark 2.1 both have "base" and "offset" fields.
        }
        return retVal;
    }
}
