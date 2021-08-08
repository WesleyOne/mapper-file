package io.github.wesleyone.store.file;

import java.io.IOException;

/**
 * 数据映射文件操作接口
 *
 * <p>核心方法：
 * <p>
 * <p><code>start</code>        启动
 * <p><code>appendEntry</code>  追加数据
 * <p><code>selectEntry</code>  查询数据
 * <p>
 *
 * @author http://wesleyone.github.io/
 */
public interface MapperFile<E> {

    /**
     * 启动
     * <p>包括初始化、恢复映射文件写坐标</p>
     * @throws IOException  IO异常
     */
    void start() throws IOException;

    /**
     * 追加数据
     * @param e 数据
     * @return  追加结果
     */
    int appendEntry(E e);

    /**
     * 查询数据
     * @param position  起始位置
     * @return  数据
     */
    E selectEntry(int position);

}
