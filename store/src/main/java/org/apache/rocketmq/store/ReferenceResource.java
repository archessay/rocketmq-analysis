/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.store;

import java.util.concurrent.atomic.AtomicLong;

public abstract class ReferenceResource {
    /**
     * 资源引用计数。
     *
     * 需要注意的是当引用计数小于等于0（refCount <= 0）或者资源不可用（available = false）时，就不再允许后续再占用该资源，因为此时可能正在执行堆外内存的回收。
     */
    protected final AtomicLong refCount = new AtomicLong(1);

    /**
     * 资源是否可用，默认为true
     */
    protected volatile boolean available = true;

    /**
     * 是否已回收堆外内存，默认false
     */
    protected volatile boolean cleanupOver = false;

    /**
     * 首次执行关闭资源的时间
     */
    private volatile long firstShutdownTimestamp = 0;

    /**
     * 执行资源占用。
     *
     * 需要注意的是当引用计数小于等于0或者资源不可用时，就不再允许该操作，因为此时可能正在执行堆外内存的回收。
     *
     * @return 是否成功占用资源
     */
    public synchronized boolean hold() {
        if (this.isAvailable()) { // 资源可用
            if (this.refCount.getAndIncrement() > 0) {
                return true;
            } else { // 引用计数小于等于0，不再允许该操作，回滚引用计数
                this.refCount.getAndDecrement();
            }
        }

        return false;
    }

    /**
     * 资源是否可用
     *
     * @return
     */
    public boolean isAvailable() {
        return this.available;
    }

    /**
     * 第一次调用时设置{@link #available}为false，设置首次执行关闭资源的时间，并释放资源占用；
     *
     * 之后再调用的时候，如果{@link #refCount} > 0，且超过了强制间隔，则设置{@link #refCount}为一个负数，并释放资源占用；
     *
     * 备注：如果在intervalForcibly期间内再次shutdown代码不会执行任何逻辑。
     *
     * 第一次调用该方法，会等待所有的资源占用都释放后，最后执行回收堆外内存；
     * 而第二次及以后调用，则会立刻执行回收堆外内存；
     *
     * @param intervalForcibly 强制间隔，即两次生效的间隔至少要有这么大(不是至多!)
     */
    public void shutdown(final long intervalForcibly) {
        if (this.available) {
            this.available = false; // 设置资源不可用
            this.firstShutdownTimestamp = System.currentTimeMillis(); // 设置首次执行关闭资源的时间
            this.release(); // 释放资源占用
        } else if (this.getRefCount() > 0) { // 若资源引用计数还大于0
            if ((System.currentTimeMillis() - this.firstShutdownTimestamp) >= intervalForcibly) { // 要超过强制间隔的阈值才行
                this.refCount.set(-1000 - this.getRefCount());
                this.release(); // 再次释放资源占用
            }
        }
    }

    /**
     * 释放资源。
     *
     * 注意，{@link #shutdown}方法会导致最后执行的{@link #release}发现{@link #refCount}小于等于0，从而调用{@link #cleanup}释放堆外内存{@link MappedFile#mappedByteBuffer}。
     */
    public void release() {
        long value = this.refCount.decrementAndGet();
        if (value > 0)
            return;

        // 回收堆外内存
        synchronized (this) {

            this.cleanupOver = this.cleanup(value);
        }
    }

    public long getRefCount() {
        return this.refCount.get();
    }

    /**
     * 回收堆外内存
     *
     * @param currentRef
     * @return
     */
    public abstract boolean cleanup(final long currentRef);

    /**
     * 是否已回收堆外内存
     *
     * @return
     */
    public boolean isCleanupOver() {
        return this.refCount.get() <= 0 && this.cleanupOver;
    }
}
