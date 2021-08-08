package io.github.wesleyone.store.file;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 映射文件操作抽象类
 * <p>实现类指定业务元素类E</p>
 * <p>实现以下方法：</p>
 * <ul>
 *     <li>encode 数据转字节数组</li>
 *     <li>decode 缓冲区转数据</li>
 * </ul>
 * @author http://wesleyone.github.io/
 */
public abstract class AbstractMapperFile<E> implements MapperFile<E> {

    private static final Logger log = LoggerFactory.getLogger(MapperFile.class.getName());
    public static final int OS_PAGE_SIZE = 1024 * 4;
    /**
     * 文件路径
     */
    private final String filePath;
    /**
     * 文件名称
     */
    private final String fileName;
    /**
     * 文件大小
     * <p>范围受限：{@link FileChannel#map}创建的文件最大size为{@code (0,Integer.MAX_VALUE]}</p>
     */
    private final int fileSize;
    /**
     * 文件
     * <p>结合NIO.2的Path创建
     */
    private File file;
    /**
     * NIO文件映射通道和缓冲区
     * <p>结合NIO.2的Path创建{@link FileChannel#open(Path, OpenOption...)}
     */
    private FileChannel fileChannel;
    /**
     * 内存文件映射的直接缓冲区
     * <p>由{@link FileChannel#map}创建
     */
    private MappedByteBuffer mappedByteBuffer;
    /**
     * 写入位置计数器
     */
    protected final AtomicInteger wrotePosition = new AtomicInteger(0);
    /**
     * 已刷盘位置计数器
     */
    private final AtomicInteger flushedPosition = new AtomicInteger(0);
    /**
     * 定时刷盘
     */
    private ScheduledExecutorService flushScheduledService = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread thread = new Thread(r);
        thread.setName("MF-Flush-thread");
        thread.setDaemon(false);
        return thread;
    });

    /**
     * 构建映射文件
     *
     * @param filePath      文件路径
     * @param fileName      文件名称
     * @param fileSize      文件大小
     */
    public AbstractMapperFile(final String filePath,final String fileName,final int fileSize) {
        this.filePath = filePath;
        this.fileName = fileName;
        this.fileSize = fileSize;

    }

    @Override
    public void start() throws IOException {
        init();
        recover();
        flushScheduledService.scheduleAtFixedRate(() -> flush(0), 200, 200, TimeUnit.MILLISECONDS);
    }

    /**
     * 初始化
     * @throws IOException IO异常
     */
    public void init() throws IOException {
        boolean isOk = false;
        try {
            Path path = Paths.get(this.filePath, this.fileName);
            ensureDirExist(path);
            // 创建文件通道。文件不存在则创建，文件通道可读、可写。
            this.fileChannel = FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
            // 创建内存文件映射直接缓冲区。
            this.mappedByteBuffer = this.fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, this.fileSize);
            isOk = true;
        } catch (IOException e) {
            log.error("Failed to init file [{} {}].",this.filePath,this.fileName, e);
            throw e;
        } finally {
            if (!isOk && this.fileChannel != null) {
                this.fileChannel.close();
            }
        }
    }

    /**
     * 恢复
     */
    public void recover() {
        int position = recoverPosition();
        wrotePosition.set(position);
        flushedPosition.set(position);
        log.info("recover file:[{} {}] position:{}",filePath, fileName, position);
    }

    /**
     * 恢复position
     * @return  当前文件的position
     */
    public int recoverPosition() {
        MappedByteBuffer mappedByteBuffer = getMappedByteBuffer();
        ByteBuffer byteBuffer = mappedByteBuffer.slice();
        while (true) {
            E e = decode(byteBuffer);
            if (e == null) {
                break;
            }
        }
        return byteBuffer.position();
    }

    /**
     * 追加数据
     * @param data 追加数据
     * @return  true成功
     */
    public synchronized int append(byte[] data) {
        int currentPos = this.wrotePosition.get();
        if ((currentPos + data.length) > this.fileSize) {
            throw new IndexOutOfBoundsException("file size is not enough.");
        }
        ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
        byteBuffer.position(currentPos);
        byteBuffer.put(data, 0, data.length);
        this.wrotePosition.addAndGet(data.length);
        return currentPos;
    }

    /**
     * 查询数据
     * @param pos   起始位置
     * @param size  查询长度
     * @return  返回数据
     */
    public synchronized ByteBuffer select(int pos, int size) {
        int readPosition = this.wrotePosition.get();
        if ((pos + size) > readPosition) {
            log.warn("select request pos invalid, request pos={} size={} readPos={}", pos, size, readPosition);
            return null;
        }
        ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
        byteBuffer.position(pos);
        ByteBuffer byteBufferNew = byteBuffer.slice();
        byteBufferNew.limit(size);
        return byteBufferNew;
    }

    /**
     * 持久化数据
     * @param leastPages    刷盘最少分页门槛
     * @return 返回最大已写位置
     */
    public synchronized int flush(int leastPages) {
        if (!isAbleToFlush(leastPages)) {
            return this.flushedPosition.get();
        }
        int value = this.wrotePosition.get();
        try {
            this.mappedByteBuffer.force();
        } catch (Throwable e) {
            log.error("Error occurred when force data to disk.", e);
        }
        this.flushedPosition.set(value);
        return this.flushedPosition.get();
    }

    /**
     * 删除文件
     */
    public synchronized void delete() {
        try {
            this.fileChannel.close();
            log.info("close file channel [{} {}] OK",this.filePath,this.fileName);

            long beginTime = System.currentTimeMillis();
            boolean result = this.file.delete();
            log.info("delete file [{}] RESULT:{} W:{} F:{} COST:{}ms"
                    , this.fileName, (result ? " OK, " : " Failed, ")
                    ,this.wrotePosition.get(),this.flushedPosition.get()
                    , System.currentTimeMillis() - beginTime);
            Thread.sleep(10);
        } catch (Exception e) {
            log.warn("close file channel [{} {}] Failed.",this.filePath,this.fileName, e);
        }
    }

    /**
     * 获取内存文件映射直接缓冲区
     * @return  内存文件映射直接缓冲区
     */
    public MappedByteBuffer getMappedByteBuffer() {
        return this.mappedByteBuffer;
    }

    @Override
    public int appendEntry(E e) {
        byte[] bytes = encode(e);
        return append(bytes);
    }

    /**
     * 查询元素
     * @param pos   查询起始position
     * @return      查询结果
     */
    @Override
    public E selectEntry(int pos) {
        int readPosition = this.wrotePosition.get();
        if (pos >= readPosition) {
            log.warn("select request pos invalid, request pos={} readPos={}", pos, readPosition);
            return null;
        }
        ByteBuffer byteBuffer = select(pos, readPosition - pos);
        if (byteBuffer == null) {
            return null;
        }
        return decode(byteBuffer);
    }

    /**
     * 对象转化成字节数组
     * @param e 对象
     * @return  字节数组
     */
    public abstract byte[] encode(E e);

    /**
     * 字节数组转化成对象
     * @param byteBuffer 字节缓冲区
     * @return      对象
     */
    public abstract E decode(ByteBuffer byteBuffer);

    /**
     * 文件目录不存在时创建
     * @param path      文件Path
     * @throws IOException  创建失败
     */
    private void ensureDirExist(Path path) throws IOException {
        this.file = path.toFile();
        if (!file.getParentFile().exists()) {
            boolean mkdirSuccess = file.getParentFile().mkdirs();
            if (!mkdirSuccess) {
                log.error("Failed to mkdir file path [{}].",this.filePath);
                throw new IOException("Failed to mkdir file path");
            }
        }
    }

    /**
     * 刷盘
     * <p>flushLeastPages=0，直接刷盘</p>
     * <p>flushLeastPages>0，满页刷盘</p>
     * @param flushLeastPages   刷盘最少页数
     * @return  true允许刷盘
     */
    private boolean isAbleToFlush(final int flushLeastPages) {
        int flushedPos = this.flushedPosition.get();
        int writePos = this.wrotePosition.get();

        if (this.isFull()) {
            return writePos > flushedPos;
        }

        if (flushLeastPages > 0) {
            return ((writePos / OS_PAGE_SIZE) - (flushedPos / OS_PAGE_SIZE)) >= flushLeastPages;
        }

        return writePos > flushedPos;
    }

    public boolean isFull() {
        return this.fileSize == this.wrotePosition.get();
    }
}
