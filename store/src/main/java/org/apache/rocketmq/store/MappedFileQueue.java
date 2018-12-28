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

import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;

public class MappedFileQueue {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    private static final InternalLogger LOG_ERROR = InternalLoggerFactory.getLogger(LoggerName.STORE_ERROR_LOGGER_NAME);

    /**
     * 每次触发删除过期文件，最多删除多少个文件
     */
    private static final int DELETE_FILES_BATCH_MAX = 10;

    /**
     * CommitLog文件的存储路径
     */
    private final String storePath;

    /**
     * 一个映射文件{@link MappedFile}的大小
     * <p>
     * 对于CommitLog文件，见{@link org.apache.rocketmq.store.config.MessageStoreConfig#mapedFileSizeCommitLog}，默认1G。
     * 对于ConsumeQueue文件，见{@link org.apache.rocketmq.store.config.MessageStoreConfig#mapedFileSizeConsumeQueue}，默认30W * 20字节。
     */
    private final int mappedFileSize;

    /**
     * MappedFileQueue所维护的所有映射文件{@link MappedFile}集合
     */
    private final CopyOnWriteArrayList<MappedFile> mappedFiles = new CopyOnWriteArrayList<MappedFile>();

    /**
     * 预分配映射文件的服务线程，RocketMQ使用内存映射处理CommitLog，ConsumeQueue文件
     */
    private final AllocateMappedFileService allocateMappedFileService;

    /**
     * 当前已刷盘的物理位置（全局）
     */
    private long flushedWhere = 0;

    /**
     * 当前已提交的物理位置（全局）
     * 所谓提交就是将{@link MappedFile#writeBuffer}的脏数据写到{@link MappedFile#fileChannel}
     */
    private long committedWhere = 0;

    /**
     * 采用完全刷盘方式（flushLeastPages为0）时，所刷盘的最后一条消息存储的时间戳
     */
    private volatile long storeTimestamp = 0;

    /**
     * 构造函数
     *
     * @param storePath                 CommitLog文件的存储路径
     * @param mappedFileSize            CommitLog文件大小，默认1G
     * @param allocateMappedFileService MappedFile分配线程，RocketMQ使用内存映射处理commitLog，consumeQueue文件
     */
    public MappedFileQueue(final String storePath, int mappedFileSize,
                           AllocateMappedFileService allocateMappedFileService) {
        this.storePath = storePath;
        this.mappedFileSize = mappedFileSize;
        this.allocateMappedFileService = allocateMappedFileService;
    }

    /**
     * checkSelf自检
     * <p>
     * 检查mappedFiles中，除去最后一个文件，其余每一个mappedFile的大小是否是mappedFileSize。
     */
    public void checkSelf() {

        if (!this.mappedFiles.isEmpty()) {
            Iterator<MappedFile> iterator = mappedFiles.iterator();
            MappedFile pre = null;
            while (iterator.hasNext()) {
                MappedFile cur = iterator.next();

                if (pre != null) {
                    if (cur.getFileFromOffset() - pre.getFileFromOffset() != this.mappedFileSize) {
                        LOG_ERROR.error("[BUG]The mappedFile queue's data is damaged, the adjacent mappedFile's offset don't match. pre file {}, cur file {}",
                                pre.getFileName(), cur.getFileName());
                    }
                }
                pre = cur;
            }
        }
    }

    /**
     * 获取在指定时间点后更新的文件，如果找不到，就返回最后一个
     *
     * @param timestamp
     * @return
     */
    public MappedFile getMappedFileByTime(final long timestamp) {
        Object[] mfs = this.copyMappedFiles(0);

        if (null == mfs)
            return null;

        for (int i = 0; i < mfs.length; i++) {
            MappedFile mappedFile = (MappedFile) mfs[i];
            if (mappedFile.getLastModifiedTimestamp() >= timestamp) {
                return mappedFile;
            }
        }

        return (MappedFile) mfs[mfs.length - 1];
    }

    private Object[] copyMappedFiles(final int reservedMappedFiles) {
        Object[] mfs;

        if (this.mappedFiles.size() <= reservedMappedFiles) {
            return null;
        }

        mfs = this.mappedFiles.toArray();
        return mfs;
    }

    public void truncateDirtyFiles(long offset) {
        List<MappedFile> willRemoveFiles = new ArrayList<MappedFile>();

        for (MappedFile file : this.mappedFiles) {
            long fileTailOffset = file.getFileFromOffset() + this.mappedFileSize;
            if (fileTailOffset > offset) {
                if (offset >= file.getFileFromOffset()) {
                    file.setWrotePosition((int) (offset % this.mappedFileSize));
                    file.setCommittedPosition((int) (offset % this.mappedFileSize));
                    file.setFlushedPosition((int) (offset % this.mappedFileSize));
                } else {
                    file.destroy(1000);
                    willRemoveFiles.add(file);
                }
            }
        }

        this.deleteExpiredFile(willRemoveFiles);
    }

    void deleteExpiredFile(List<MappedFile> files) {

        if (!files.isEmpty()) {

            Iterator<MappedFile> iterator = files.iterator();
            while (iterator.hasNext()) {
                MappedFile cur = iterator.next();
                if (!this.mappedFiles.contains(cur)) {
                    iterator.remove();
                    log.info("This mappedFile {} is not contained by mappedFiles, so skip it.", cur.getFileName());
                }
            }

            try {
                if (!this.mappedFiles.removeAll(files)) {
                    log.error("deleteExpiredFile remove failed.");
                }
            } catch (Exception e) {
                log.error("deleteExpiredFile has exception.", e);
            }
        }
    }

    /**
     * 对MappedFileQueue中的mappedFiles进行初始化，
     * <p>
     * 方法首先会根据文件名称对commitLog文件进行升序排序，然后丢弃大小不为mappedFileSize的文件及其后续文件。
     *
     * @return
     */
    public boolean load() {
        File dir = new File(this.storePath); // CommitLog文件的存储目录
        File[] files = dir.listFiles();
        if (files != null) {
            // ascending order
            Arrays.sort(files);
            for (File file : files) {

                if (file.length() != this.mappedFileSize) {
                    log.warn(file + "\t" + file.length()
                            + " length not matched message store config value, ignore it");
                    return true;
                }

                try {
                    MappedFile mappedFile = new MappedFile(file.getPath(), mappedFileSize);

                    mappedFile.setWrotePosition(this.mappedFileSize);
                    mappedFile.setFlushedPosition(this.mappedFileSize);
                    mappedFile.setCommittedPosition(this.mappedFileSize);
                    this.mappedFiles.add(mappedFile);
                    log.info("load " + file.getPath() + " OK");
                } catch (IOException e) {
                    log.error("load file " + file + " error", e);
                    return false;
                }
            }
        }

        return true;
    }

    /**
     * 获取还未刷盘的消息大小
     *
     * @return
     */
    public long howMuchFallBehind() {
        if (this.mappedFiles.isEmpty())
            return 0;

        long committed = this.flushedWhere; // 当前已刷盘的位置（全局）
        if (committed != 0) {
            MappedFile mappedFile = this.getLastMappedFile(0, false); // 获取最后一个MappedFile
            if (mappedFile != null) {
                return (mappedFile.getFileFromOffset() + mappedFile.getWrotePosition()) - committed;
            }
        }

        return 0;
    }

    /**
     * 该方法的实现逻辑是这样的，首先是获取{@link MappedFileQueue}维护的最后一个映射文件，
     * <p>
     * 如果{@code needCreate}为true，当映射文件不存在或者获取的映射文件已写满，会计算新的映射文件的起始物理偏移量（该偏移量会用作映射文件所对应的CommitLog文件的文件名），
     * 然后通过{@link AllocateMappedFileService}预分配映射文件服务线程来获取新的映射文件。
     * <p>
     * 否则直接返回所获取的最后一个映射文件。
     *
     * @param startOffset
     * @param needCreate  是否创建新的映射文件
     * @return
     */
    public MappedFile getLastMappedFile(final long startOffset, boolean needCreate) {
        long createOffset = -1;
        // 获取最后一个映射文件，如果其为null或者已写满，会走创建逻辑
        MappedFile mappedFileLast = getLastMappedFile();

        // 最后一个映射文件为null，也即mappedFiles为空，创建一个新的映射文件（这也是第一个映射文件）
        if (mappedFileLast == null) {
            // 计算将要创建的映射文件的物理偏移量
            // 如果指定的startOffset不足mappedFileSize，则从offset 0开始；
            // 否则，从为mappedFileSize整数倍的offset开始；
            createOffset = startOffset - (startOffset % this.mappedFileSize);
        }

        // 最后一个映射文件已经写满了，创建一个新的映射文件
        if (mappedFileLast != null && mappedFileLast.isFull()) {
            // 计算将要创建的映射文件的物理偏移量
            // 该映射文件的物理偏移量等于上一CommitLog文件的起始偏移量加上CommitLog文件大小
            createOffset = mappedFileLast.getFileFromOffset() + this.mappedFileSize;
        }

        // 创建新的映射文件
        if (createOffset != -1 && needCreate) {
            // 构造CommitLog文件名称
            String nextFilePath = this.storePath + File.separator + UtilAll.offset2FileName(createOffset); // @1
            String nextNextFilePath = this.storePath + File.separator
                    + UtilAll.offset2FileName(createOffset + this.mappedFileSize);
            MappedFile mappedFile = null;

            // 优先通过AllocateMappedFileService创建映射文件，因为是预分配方式，性能很高。
            // 如果上述方式分配失败，再通过new创建映射文件。
            if (this.allocateMappedFileService != null) { // @2
                mappedFile = this.allocateMappedFileService.putRequestAndReturnMappedFile(nextFilePath,
                        nextNextFilePath, this.mappedFileSize);
            } else {
                try {
                    mappedFile = new MappedFile(nextFilePath, this.mappedFileSize);
                } catch (IOException e) {
                    log.error("create mappedFile exception", e);
                }
            }

            if (mappedFile != null) {
                if (this.mappedFiles.isEmpty()) {
                    mappedFile.setFirstCreateInQueue(true);
                }
                this.mappedFiles.add(mappedFile);
            }

            return mappedFile;
        }

        return mappedFileLast;
    }

    /**
     * 获取最后一个MappedFile，如果其为null或者已写满，则会创建新的MappedFile
     *
     * @param startOffset
     * @return
     */
    public MappedFile getLastMappedFile(final long startOffset) {
        return getLastMappedFile(startOffset, true);
    }

    /**
     * 获取最后一个MappedFile
     *
     * @return
     */
    public MappedFile getLastMappedFile() {
        MappedFile mappedFileLast = null;

        while (!this.mappedFiles.isEmpty()) {
            try {
                mappedFileLast = this.mappedFiles.get(this.mappedFiles.size() - 1);
                break;
            } catch (IndexOutOfBoundsException e) {
                //continue;
            } catch (Exception e) {
                log.error("getLastMappedFile has exception.", e);
                break;
            }
        }

        return mappedFileLast;
    }

    public boolean resetOffset(long offset) {
        MappedFile mappedFileLast = getLastMappedFile();

        if (mappedFileLast != null) {
            long lastOffset = mappedFileLast.getFileFromOffset() +
                    mappedFileLast.getWrotePosition();
            long diff = lastOffset - offset;

            final int maxDiff = this.mappedFileSize * 2;
            if (diff > maxDiff)
                return false;
        }

        ListIterator<MappedFile> iterator = this.mappedFiles.listIterator();

        while (iterator.hasPrevious()) {
            mappedFileLast = iterator.previous();
            if (offset >= mappedFileLast.getFileFromOffset()) {
                int where = (int) (offset % mappedFileLast.getFileSize());
                mappedFileLast.setFlushedPosition(where);
                mappedFileLast.setWrotePosition(where);
                mappedFileLast.setCommittedPosition(where);
                break;
            } else {
                iterator.remove();
            }
        }
        return true;
    }

    public long getMinOffset() {

        if (!this.mappedFiles.isEmpty()) {
            try {
                return this.mappedFiles.get(0).getFileFromOffset();
            } catch (IndexOutOfBoundsException e) {
                //continue;
            } catch (Exception e) {
                log.error("getMinOffset has exception.", e);
            }
        }
        return -1;
    }

    /**
     * 获取最后一个MappedFile当前具有可刷盘数据的最大偏移量（全局）
     *
     * @return
     */
    public long getMaxOffset() {
        MappedFile mappedFile = getLastMappedFile(); // 获取最后一个MappedFile
        if (mappedFile != null) {
            return mappedFile.getFileFromOffset() + mappedFile.getReadPosition(); // 映射文件的起始偏移量 + 映射文件当前具有可刷盘数据的最大偏移量
        }
        return 0;
    }

    /**
     * 获取最后一个MappedFile当前写入的位置（全局）
     *
     * @return
     */
    public long getMaxWrotePosition() {
        MappedFile mappedFile = getLastMappedFile(); // 获取最后一个MappedFile
        if (mappedFile != null) {
            return mappedFile.getFileFromOffset() + mappedFile.getWrotePosition(); // 映射文件的起始偏移量 + 映射文件的当前写入位置
        }
        return 0;
    }

    /**
     * 返回待提交数据的大小
     *
     * @return
     */
    public long remainHowManyDataToCommit() {
        return getMaxWrotePosition() - committedWhere;
    }

    /**
     * 返回待刷盘数据的大小
     *
     * @return
     */
    public long remainHowManyDataToFlush() {
        return getMaxOffset() - flushedWhere;
    }

    public void deleteLastMappedFile() {
        MappedFile lastMappedFile = getLastMappedFile();
        if (lastMappedFile != null) {
            lastMappedFile.destroy(1000);
            this.mappedFiles.remove(lastMappedFile);
            log.info("on recover, destroy a logic mapped file " + lastMappedFile.getFileName());

        }
    }

    public int deleteExpiredFileByTime(final long expiredTime,
                                       final int deleteFilesInterval,
                                       final long intervalForcibly,
                                       final boolean cleanImmediately) {
        Object[] mfs = this.copyMappedFiles(0);

        if (null == mfs)
            return 0;

        int mfsLength = mfs.length - 1;
        int deleteCount = 0;
        List<MappedFile> files = new ArrayList<MappedFile>();
        if (null != mfs) {
            for (int i = 0; i < mfsLength; i++) {
                MappedFile mappedFile = (MappedFile) mfs[i];
                long liveMaxTimestamp = mappedFile.getLastModifiedTimestamp() + expiredTime;
                if (System.currentTimeMillis() >= liveMaxTimestamp || cleanImmediately) {
                    if (mappedFile.destroy(intervalForcibly)) {
                        files.add(mappedFile);
                        deleteCount++;

                        if (files.size() >= DELETE_FILES_BATCH_MAX) {
                            break;
                        }

                        if (deleteFilesInterval > 0 && (i + 1) < mfsLength) {
                            try {
                                Thread.sleep(deleteFilesInterval);
                            } catch (InterruptedException e) {
                            }
                        }
                    } else {
                        break;
                    }
                } else {
                    //avoid deleting files in the middle
                    break;
                }
            }
        }

        deleteExpiredFile(files);

        return deleteCount;
    }

    public int deleteExpiredFileByOffset(long offset, int unitSize) {
        Object[] mfs = this.copyMappedFiles(0);

        List<MappedFile> files = new ArrayList<MappedFile>();
        int deleteCount = 0;
        if (null != mfs) {

            int mfsLength = mfs.length - 1;

            for (int i = 0; i < mfsLength; i++) {
                boolean destroy;
                MappedFile mappedFile = (MappedFile) mfs[i];
                SelectMappedBufferResult result = mappedFile.selectMappedBuffer(this.mappedFileSize - unitSize);
                if (result != null) {
                    long maxOffsetInLogicQueue = result.getByteBuffer().getLong();
                    result.release();
                    destroy = maxOffsetInLogicQueue < offset;
                    if (destroy) {
                        log.info("physic min offset " + offset + ", logics in current mappedFile max offset "
                                + maxOffsetInLogicQueue + ", delete it");
                    }
                } else if (!mappedFile.isAvailable()) { // Handle hanged file.
                    log.warn("Found a hanged consume queue file, attempting to delete it.");
                    destroy = true;
                } else {
                    log.warn("this being not executed forever.");
                    break;
                }

                if (destroy && mappedFile.destroy(1000 * 60)) {
                    files.add(mappedFile);
                    deleteCount++;
                } else {
                    break;
                }
            }
        }

        deleteExpiredFile(files);

        return deleteCount;
    }

    /**
     * 根据当前刷盘位置查找MappedFile，然后对其执行刷盘操作
     *
     * @param flushLeastPages 执行刷盘的最少内存页数
     * @return 是否有新的数据刷盘。注意，false意味着有新的数据刷盘。
     */
    public boolean flush(final int flushLeastPages) {
        boolean result = true;
        // 根据当前已刷盘物理位置查找映射文件。如果找不到，且当前已刷盘物理位置为0，则返回第一个映射文件。
        // 因为flushedWhere的初始值为0，当第一个映射文件的起始偏移量不为0（当然，默认为0）时，此时根据flushedWhere是查找不到映射文件的，所以就需要返回第一个映射文件。
        MappedFile mappedFile = this.findMappedFileByOffset(this.flushedWhere, this.flushedWhere == 0);
        if (mappedFile != null) {
            long tmpTimeStamp = mappedFile.getStoreTimestamp(); // 最后一次写入消息（写入buffer）的时间戳
            int offset = mappedFile.flush(flushLeastPages); // 执行真正的刷盘，并返回当前已刷盘的位置（针对每一个MappedFile，offset从0开始）

            long where = mappedFile.getFileFromOffset() + offset;
            result = where == this.flushedWhere; // 判断是否有新的数据刷盘
            this.flushedWhere = where; // 保存当前已刷盘的位置（全局）

            if (0 == flushLeastPages) {
                this.storeTimestamp = tmpTimeStamp;
            }
        }

        return result;
    }

    /**
     * 根据当前已提交位置查找映射文件，然后对其执行提交操作。
     * <p>
     * 所谓提交就是将writeBuffer的脏数据写到fileChannel。
     *
     * @param commitLeastPages 执行提交的最少内存页数
     * @return 是否有新的数据提交。注意，false意味着有新的数据提交。
     */
    public boolean commit(final int commitLeastPages) {
        boolean result = true;
        // 根据当前已提交物理位置查找映射文件。如果找不到，且当前已提交物理位置为0，则返回第一个映射文件
        // 因为committedWhere的初始值为0，当第一个映射文件的起始偏移量不为0（当然，默认为0）时，此时根据committedWhere是查找不到映射文件的，所以就需要返回第一个映射文件
        MappedFile mappedFile = this.findMappedFileByOffset(this.committedWhere, this.committedWhere == 0);
        if (mappedFile != null) {
            int offset = mappedFile.commit(commitLeastPages); // 执行真正的提交，并返回当前已提交的位置（针对每一个映射文件，offset从0开始）
            long where = mappedFile.getFileFromOffset() + offset;
            result = where == this.committedWhere; // 判断是否有新的数据提交
            this.committedWhere = where;// 保存当前已提交的位置（全局）
        }

        return result;
    }

    /**
     * 根据物理偏移量查找映射文件。
     * <p>
     * 如果returnFirstOnNotFound为true，那么如果映射文件没有找到，则返回第一个。
     *
     * @param offset                物理偏移量
     * @param returnFirstOnNotFound 如果映射文件没有找到，是否返回第一个
     * @return MappedFile或者null(没有找到映射文件，并且returnFirstOnNotFound为false).
     */
    public MappedFile findMappedFileByOffset(final long offset, final boolean returnFirstOnNotFound) {
        try {
            MappedFile firstMappedFile = this.getFirstMappedFile(); // 获取第一个映射文件
            MappedFile lastMappedFile = this.getLastMappedFile(); // 获取最后一个映射文件
            if (firstMappedFile != null && lastMappedFile != null) {
                // 如果offset小于第一个映射文件的起始偏移量，或者offset大于等于最后一个映射文件的起始偏移量加上映射文件的大小，那么offset不匹配，这是不对的
                if (offset < firstMappedFile.getFileFromOffset() || offset >= lastMappedFile.getFileFromOffset() + this.mappedFileSize) {
                    LOG_ERROR.warn("Offset not matched. Request offset: {}, firstOffset: {}, lastOffset: {}, mappedFileSize: {}, mappedFiles count: {}",
                            offset,
                            firstMappedFile.getFileFromOffset(),
                            lastMappedFile.getFileFromOffset() + this.mappedFileSize,
                            this.mappedFileSize,
                            this.mappedFiles.size());
                } else {
                    // 这里的减法主要是考虑到第一个映射文件的起始偏移量可能不是从0开始（MappedFileQueue在创建映射文件时是支持指定第一个映射文件的起始偏移量的，默认为0）
                    int index = (int) ((offset / this.mappedFileSize) - (firstMappedFile.getFileFromOffset() / this.mappedFileSize));
                    MappedFile targetFile = null;
                    try {
                        targetFile = this.mappedFiles.get(index);
                    } catch (Exception ignored) {
                    }

                    if (targetFile != null && offset >= targetFile.getFileFromOffset()
                            && offset < targetFile.getFileFromOffset() + this.mappedFileSize) {
                        return targetFile;
                    }

                    for (MappedFile tmpMappedFile : this.mappedFiles) {
                        if (offset >= tmpMappedFile.getFileFromOffset()
                                && offset < tmpMappedFile.getFileFromOffset() + this.mappedFileSize) {
                            return tmpMappedFile;
                        }
                    }
                }

                // 如果找不到，返回第一个映射文件
                if (returnFirstOnNotFound) {
                    return firstMappedFile;
                }
            }
        } catch (Exception e) {
            log.error("findMappedFileByOffset Exception", e);
        }

        return null;
    }

    public MappedFile getFirstMappedFile() {
        MappedFile mappedFileFirst = null;

        if (!this.mappedFiles.isEmpty()) {
            try {
                mappedFileFirst = this.mappedFiles.get(0);
            } catch (IndexOutOfBoundsException e) {
                //ignore
            } catch (Exception e) {
                log.error("getFirstMappedFile has exception.", e);
            }
        }

        return mappedFileFirst;
    }

    public MappedFile findMappedFileByOffset(final long offset) {
        return findMappedFileByOffset(offset, false);
    }

    public long getMappedMemorySize() {
        long size = 0;

        Object[] mfs = this.copyMappedFiles(0);
        if (mfs != null) {
            for (Object mf : mfs) {
                if (((ReferenceResource) mf).isAvailable()) {
                    size += this.mappedFileSize;
                }
            }
        }

        return size;
    }

    public boolean retryDeleteFirstFile(final long intervalForcibly) {
        MappedFile mappedFile = this.getFirstMappedFile();
        if (mappedFile != null) {
            if (!mappedFile.isAvailable()) {
                log.warn("the mappedFile was destroyed once, but still alive, " + mappedFile.getFileName());
                boolean result = mappedFile.destroy(intervalForcibly);
                if (result) {
                    log.info("the mappedFile re delete OK, " + mappedFile.getFileName());
                    List<MappedFile> tmpFiles = new ArrayList<MappedFile>();
                    tmpFiles.add(mappedFile);
                    this.deleteExpiredFile(tmpFiles);
                } else {
                    log.warn("the mappedFile re delete failed, " + mappedFile.getFileName());
                }

                return result;
            }
        }

        return false;
    }

    public void shutdown(final long intervalForcibly) {
        for (MappedFile mf : this.mappedFiles) {
            mf.shutdown(intervalForcibly);
        }
    }

    public void destroy() {
        for (MappedFile mf : this.mappedFiles) {
            mf.destroy(1000 * 3);
        }
        this.mappedFiles.clear();
        this.flushedWhere = 0;

        // delete parent directory
        File file = new File(storePath);
        if (file.isDirectory()) {
            file.delete();
        }
    }

    public long getFlushedWhere() {
        return flushedWhere;
    }

    public void setFlushedWhere(long flushedWhere) {
        this.flushedWhere = flushedWhere;
    }

    public long getStoreTimestamp() {
        return storeTimestamp;
    }

    public List<MappedFile> getMappedFiles() {
        return mappedFiles;
    }

    public int getMappedFileSize() {
        return mappedFileSize;
    }

    public long getCommittedWhere() {
        return committedWhere;
    }

    public void setCommittedWhere(final long committedWhere) {
        this.committedWhere = committedWhere;
    }
}
