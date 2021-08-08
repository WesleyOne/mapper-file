package io.github.wesleyone.store.file;

import com.alibaba.fastjson.JSONObject;

import java.nio.ByteBuffer;

/**
 * 默认协议文件
 * <ul>
 *     <li>int(4) magic</li>
 *     <li>int(4) size</li>
 *     <li>(size) json(message)</li>
 * </ul>
 * @author http://wesleyone.github.io/
 */
public class DefaultMessageMapperFile extends AbstractMapperFile<DefaultMessage>{

    private static final int MAGIC_CODE = 1;
    /**
     * 构建映射文件
     *
     * @param filePath 文件路径
     * @param fileName 文件名称
     * @param fileSize 文件大小
     */
    public DefaultMessageMapperFile(String filePath, String fileName, int fileSize) {
        super(filePath, fileName, fileSize);
    }

    @Override
    public byte[] encode(DefaultMessage defaultMessage) {
        if (defaultMessage == null) {
            throw new IllegalArgumentException("defaultMessage is null");
        }
        byte[] bytes = JSONObject.toJSONBytes(defaultMessage);
        int totalLength = 4 + 4 + bytes.length;
        ByteBuffer byteBuffer = ByteBuffer.allocate(totalLength);
        byteBuffer.putInt(MAGIC_CODE);
        byteBuffer.putInt(bytes.length);
        byteBuffer.put(bytes);
        return byteBuffer.array();
    }

    @Override
    public DefaultMessage decode(ByteBuffer byteBuffer) {
        if (byteBuffer.position() >= byteBuffer.limit()) {
            return null;
        }
        int magic = byteBuffer.getInt();
        if (magic == 0) {
            // 说明没数据了。回拨position位置。
            byteBuffer.position(byteBuffer.position() - 4);
            return null;
        }
        if (magic != MAGIC_CODE) {
            throw new IllegalStateException("magic is illegal.");
        }
        int totalLength = byteBuffer.getInt();
        byte[] dst = new byte[totalLength];
        byteBuffer.get(dst);
        return JSONObject.parseObject(dst, DefaultMessage.class);
    }

}
