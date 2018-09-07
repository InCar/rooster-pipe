package com.incarcloud.rooster.pipe;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * PipeSlotTest
 *
 * @author Aaric, created on 2018-09-07T10:14.
 * @since 2.3.0-SNAPSHOT
 */
public class PipeSlotTest {

    @Test
    public void testGetDeviceId() {
        String mark = "china-gmmc-1.16|911111199000001";
        String deviceId = mark.split("\\|")[1];
        System.out.println(deviceId);
        Assert.assertEquals("911111199000001", deviceId);
    }

    @Test
    public void testShowMapWithLambda() {
        Map<String, String> mapDatas = new HashMap<>();
        mapDatas.put("key01", "value01");
        mapDatas.put("key02", "value02");

        mapDatas.forEach((key, value) -> {
            System.out.println(key + ": " + value);
        });

        Assert.assertEquals(2, mapDatas.size());
    }
}
