package com.incarcloud.rooster.cache;

/**
 * Created by Kong on 2017/12/20.
 */
public interface ICacheManager {

    /**
     * 保存缓存数据
     * @param key 键
     * @param value 值
     */
    void save(String key,String value) ;

    /**
     * 保存缓存数据
     * @param key 键
     * @param value 值
     * @param timeOut 超时时间
     */
    void save(String key,String value,int timeOut) ;

    /**
     * 获取缓存值
     * @param key 键
     * @return
     */
    String getValue(String key) ;

    /**
     * 通过键值删除缓存
     * @param key
     */
    void delete(String key) ;

}
