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
    // 引用计数，大于0可用，小于等于0不可用
    protected final AtomicLong refCount = new AtomicLong(1);
    // 是否可用
    protected volatile boolean available = true;
    // 是否清理
    protected volatile boolean cleanupOver = false;
    // 第一次执行shutdown的时间
    private volatile long firstShutdownTimestamp = 0;

    /**
     * 占用资源
     *
     * @return
     */
    public synchronized boolean hold() {
        if (this.isAvailable()) {
            if (this.refCount.getAndIncrement() > 0) {
                // 大于0可用，引用计数加一
                return true;
            } else {
                // 小于等于0不可用，则执行减一回滚
                this.refCount.getAndDecrement();
            }
        }

        return false;
    }

    public boolean isAvailable() {
        return this.available;
    }

    /**
     * 第一次调用时设置available为false，设置初次shutdown的时间，释放资源；
     *
     * 之后再调用的时候，如果refCount > 0，且超过了强制间隔，则设置为一个负数，释放资源；
     *
     * 备注：如果在intervalForcibly期间内再次shutdown代码不会执行任何逻辑。
     *
     * 第一次，会等待所有的资源释放后，最后执行清理；
     * 而第二次及以后调用，则会立刻执行清理；
     *
     * @param intervalForcibly 强制间隔，即两次生效的间隔至少要有这么大(不是至多!)
     */
    public void shutdown(final long intervalForcibly) {
        if (this.available) {
            this.available = false; // 改为不可用
            this.firstShutdownTimestamp = System.currentTimeMillis(); // 设置初次shutdown的时间
            this.release(); // 释放资源
        } else if (this.getRefCount() > 0) { // 若引用计数还大于0
            if ((System.currentTimeMillis() - this.firstShutdownTimestamp) >= intervalForcibly) { // 要超过强制间隔的阈值才行
                this.refCount.set(-1000 - this.getRefCount());
                this.release(); // 再次释放资源
            }
        }
    }

    /**
     * 释放堆外内存。
     *
     * 注意，shutdown会导致最后执行的release发现refCount小于等于0，从而调用cleanup释放堆外内存mappedByteBuffer。
     */
    public void release() {
        long value = this.refCount.decrementAndGet();
        if (value > 0)
            return;

        synchronized (this) {

            this.cleanupOver = this.cleanup(value);
        }
    }

    public long getRefCount() {
        return this.refCount.get();
    }

    /**
     * 资源清理
     *
     * @param currentRef
     * @return
     */
    public abstract boolean cleanup(final long currentRef);

    /**
     * 是否清理
     *
     * @return
     */
    public boolean isCleanupOver() {
        return this.refCount.get() <= 0 && this.cleanupOver;
    }
}
