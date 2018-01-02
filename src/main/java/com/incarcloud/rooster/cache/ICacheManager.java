package com.incarcloud.rooster.cache;

/**
 * 缓存管理器
 *
 * @author Kong, Created on 2017/12/20.
 * @since 2.1.4-SNAPSHOT
 */
public interface ICacheManager {

    /**
     * 设置缓存数据
     *
     * @param key   键名
     * @param value 键值
     */
    void set(String key, String value);

    /**
     * 设置缓存数据
     *
     * @param key   键名
     * @param value 键值
     * @param ttl   超时时间，单位：秒
     */
    void set(String key, String value, int ttl);

    /**
     * 设置超时时间
     *
     * @param key 键名
     * @param ttl 超时时间，单位：秒
     */
    void expire(String key, int ttl);

    /**
     * 根据key获取数据
     *
     * @param key 键名
     * @return
     */
    String get(String key);

    /**
     * 根据key移除数据
     *
     * @param key 键名
     */
    void delete(String key);
}
