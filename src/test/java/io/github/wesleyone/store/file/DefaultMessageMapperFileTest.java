package io.github.wesleyone.store.file;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;

/**
 * @author http://wesleyone.github.io/
 */
public class DefaultMessageMapperFileTest {

    @Test
    public void test() throws IOException, InterruptedException {
        int size = 1 << 16;
        DefaultMessageMapperFile mapperFile = new DefaultMessageMapperFile("./target/store", ""+size, size);
        mapperFile.start();

        for (int i=0;i<50;i++) {
            DefaultMessage message = new DefaultMessage();
            message.setTitle("title"+i);
            message.setBody("body"+ ThreadLocalRandom.current().nextInt(0,1000));
            int position = mapperFile.appendEntry(message);
            System.out.println("position:"+position);
            Thread.sleep(500L);
            DefaultMessage selectMessage = mapperFile.selectEntry(position);
            System.out.println(selectMessage);
            Assert.assertEquals(message.getTitle(), selectMessage.getTitle());
            Assert.assertEquals(message.getBody(), selectMessage.getBody());
        }
    }
}