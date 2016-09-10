/**
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
package com.github.fhuss.kafka.influxdb.internal;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.OutputStream;
import java.nio.ByteBuffer;

public class ByteBufferOutputStream extends OutputStream {

    private static final Logger LOG = LoggerFactory.getLogger(ByteBufferOutputStream.class);

    private static final float REALLOCATION_FACTOR = 1.1F;

    private ByteBuffer buffer;
    private long numBytes = 0L;

    public ByteBufferOutputStream(ByteBuffer buffer) {
        this.buffer = buffer;
    }

    public void write(int b) {
        if(this.buffer.remaining() < 1) {
            this.expandBuffer(this.buffer.capacity() + 1);
        }

        this.buffer.put((byte)b);
        ++this.numBytes;
    }

    public void write(byte[] bytes, int off, int len) {
        if(this.buffer.remaining() < len) {
            this.expandBuffer(this.buffer.capacity() + len);
        }

        this.buffer.put(bytes, off, len);
        this.numBytes += len - off;
    }

    public long getNumBytesWritten() {
        return this.numBytes;
    }

    public ByteBuffer buffer() {
        return this.buffer;
    }

    private void expandBuffer(int size) {
        LOG.info("Expand buffer with new capacity {}", size);
        int expandSize = Math.max((int)((float)this.buffer.capacity() * REALLOCATION_FACTOR), size);
        ByteBuffer temp = ByteBuffer.allocate(expandSize);
        temp.put(this.buffer.array(), this.buffer.arrayOffset(), this.buffer.position());
        this.buffer = temp;
    }
}
