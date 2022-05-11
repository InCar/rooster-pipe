package com.incarcloud.rooster.pipe;

import com.incarcloud.rooster.cache.ICacheManager;
import com.incarcloud.rooster.datapack.*;
import com.incarcloud.rooster.mq.IBigMQ;
import com.incarcloud.rooster.mq.MQMsg;
import com.incarcloud.rooster.share.Constants;
import com.incarcloud.rooster.util.DataPackObjectUtil;
import com.incarcloud.rooster.util.GsonFactory;
import com.incarcloud.rooster.util.RowKeyUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Xiong Guanghua
 * @Description: 管道槽, 一个管道槽对应一个队列
 * @date 2017年6月2日 下午3:55:17
 */
public class PipeSlot {
    static final int maxBlockingNum = 300000;
    static final AtomicInteger blockingNum = new AtomicInteger();
    static final AtomicInteger consumeNum = new AtomicInteger();
    static final AtomicInteger processNum = new AtomicInteger();
    static final AtomicInteger saveNum = new AtomicInteger();
    static final AtomicInteger errorNum = new AtomicInteger();

    static ScheduledExecutorService pool_monitor;

    static {
        startMonitor();
    }

    static final ConcurrentHashMap<String, String> cache = new ConcurrentHashMap<>();

    private static final ThreadPoolExecutor pool_work = (ThreadPoolExecutor) Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() << 1);
    private static final ThreadPoolExecutor pool_save = (ThreadPoolExecutor) Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() << 3);

    private static void startMonitor() {
        pool_monitor = Executors.newSingleThreadScheduledExecutor();
        pool_monitor.scheduleAtFixedRate(() -> {
            final int count1 = consumeNum.getAndSet(0);
            final int count2 = processNum.getAndSet(0);
            final int count3 = saveNum.getAndSet(0);
            final int count4 = errorNum.getAndSet(0);
            s_logger.info("-------blockingNum:{} consumeSpeed:{} blocking:{} processSpeed:{}/{} blocking:{} saveSpeed:{}",
                    blockingNum.get(), count1 / 3, pool_work.getQueue().size(), count2 / 3, count4 / 3, pool_save.getQueue().size(), count3 / 3);
        }, 3, 3, TimeUnit.SECONDS);
    }


    /**
     * Logger
     */
    private static final Logger s_logger = LoggerFactory.getLogger(PipeSlot.class);


    /**
     * 名称
     */
    private String name;

    /**
     * 采集槽所在主机
     */
    private PipeHost _host;

    /**
     * @param host 采集槽所在主机
     */
    public PipeSlot(PipeHost host) {
        _host = host;
        this.name = _host.getName() + "-" + "slot" + new Date().getTime();
    }

    /**
     * @param name  采集槽名称
     * @param _host 采集槽所在主机
     */
    public PipeSlot(String name, PipeHost _host) {
        this.name = name;
        this._host = _host;
    }

    /**
     * 启动
     */
    public void start() {
        s_logger.info(name + " start receive message!!!");
        run();
        s_logger.info(name + " start success!!!");
    }


    public void run() {
        // 只获取MQ消息放入队列，不进行别的操作，为了加快消费MQ消息
        final IBigMQ receiveDataMQ = _host.getReceiveDataMQ();
        try {
            while (true) {
                // 消费MQ消息
                List<byte[]> msgList = receiveDataMQ.batchReceive(_host.getReceiveDataTopic(), 200);
                if (msgList != null && !msgList.isEmpty()) {
                    consumeNum.addAndGet(msgList.size());
                    blockingNum.addAndGet(msgList.size());
                    for (byte[] bytes : msgList) {
                        pool_work.execute(() -> {
                            dealMQMsg(bytes);
                        });
                    }

                }
                while (blockingNum.get() >= maxBlockingNum) {
                    //如果达到最大数量、休眠100ms
                    TimeUnit.MILLISECONDS.sleep(100);
                }
            }
        } catch (InterruptedException ex) {
            s_logger.error("error", ex);
        }

        // 停止后释放连接
        receiveDataMQ.releaseCurrentConn(_host.getReceiveDataTopic());
    }


    /**
     * 处理MQ消息
     *
     * @param msg 消息列表
     */
    private void dealMQMsg(byte[] msg) {
        // 处理消息
        try {
            // string转到datapack
            String json = new String(msg);
            MQMsg m = GsonFactory.newInstance().createGson().fromJson(json, MQMsg.class);
            DataPack dp = DataPack.deserializeFromBytes(m.getData());
            s_logger.debug("DataPack: {}", dp.toString());

            // 获得解析器
            IDataParser dataParser = DataParserManager.getDataParser(dp.getProtocol());
            if (null == dataParser) {
                s_logger.error("Not support: {}!!!", dp.getProtocol());
                return;
            }

            // 调用解析器解析完整报文
            List<DataPackTarget> dataPackTargetList = dataParser.extractBody(dp);// 同一个DataPack解出的数据列表
            if (null == dataPackTargetList || 0 == dataPackTargetList.size()) {
                s_logger.info("extractBody: null, dataPackTargetList: {}, DataPack: {}", m, dp);
                return;
            }

            // 获取消息中传过来的deviceId
            String deviceId = m.getMark().split("\\|")[1];
            if (StringUtils.isBlank(deviceId)) {
                s_logger.error("Invalid data: no deviceId!", deviceId);
                return;
            }

            // 从缓存提取VIN信息
            final String vin = cache.computeIfAbsent(deviceId, k -> cacheManager.hget(Constants.CacheNamespaceKey.CACHE_DEVICE_ID_HASH, k));
            if (StringUtils.isBlank(vin)) {
                s_logger.error("Invalid deviceId({}): no vin!", deviceId);
                return;
            }

            // 完善DataPack信息，主要是车架号
            dataPackTargetList.forEach(object -> {
                if (null != object.getDataPackObject()) {
                    // 完善车架号
                    object.getDataPackObject().setVin(vin);
                }
            });
            processNum.incrementAndGet();

            //异步保存
            pool_save.execute(() -> {
                // 永久保存数据到BigTable
                Map<String, DataPackObject> mapDataPackObjects = saveDataPacks(vin, dataPackTargetList, dp.getReceiveTime());
                // 分发数据
                if (PipeHost.DEFAULT_HOST_ROLE.equals(_host.getRole())) {
                    // 只有主节点支持数据分发
                    dispatchDataPacks(dp, mapDataPackObjects);
                }
                saveNum.incrementAndGet();
                blockingNum.decrementAndGet();
            });


        } catch (Exception e) {
            blockingNum.decrementAndGet();
            errorNum.incrementAndGet();
            s_logger.error("Deal with msg error, {}", new String(msg), e);
        }
    }


    /**
     * 纠正检测时间和维护车辆状态缓存
     * 1.设置网关接收时间<br />
     * 2.对采集时间字段为空或无效的数据进行处理<br>
     * 3.生成rowkey，返回 rowkey -> DataPackObject
     *
     * @param dataPackTargetList 车辆数据列表(同一个DataPack解出的)
     * @param receiveTime        网关接收时间（gather服务器接收时间，非设备检测时间）
     * @param vin                车架号
     * @return 待保存的数据
     */
    private Map<String, DataPackObject> checkDetectionTimeAndDealCachePool(List<DataPackTarget> dataPackTargetList, Date receiveTime, String vin) {
        // 初始化返回对象
        Map<String, DataPackObject> mapDataPackObjects = new HashMap<>();

        // 生成rowKey和datapack之间的关系数据
        for (DataPackTarget target : dataPackTargetList) {
            // 1.设置网关接收时间
            DataPackObject dataPackObject = target.getDataPackObject();
            dataPackObject.setReceiveTime(receiveTime);
            String dataType = DataPackObjectUtil.getDataType(dataPackObject);// 数据类型

            // 2.判断检测时间无效情况
            Date detectionTime = target.getDataPackObject().getDetectionTime();
            if (DataPackObjectUtil.isLegalDetectionDate(detectionTime)) {
                // 判断依据：比当前时间晚1个月或者早30分钟视为无效数据，主动丢弃
                s_logger.info("Legal detection date data: {}", DataPackObjectUtil.toJson(target.getDataPackObject()));
                continue;
            }


            // 3.创建rowkey和datapack关系
            String detectionTimeString = DataPackObjectUtil.convertDetectionTimeToString(detectionTime);
            String rowKey = RowKeyUtil.makeRowKey(vin, dataType, detectionTimeString);

            mapDataPackObjects.put(rowKey, dataPackObject);

            // 4、监控车辆状态信息
            operationCachePool(dataPackObject);
        }

        return mapDataPackObjects;
    }

    /**
     * 保存数据
     *
     * @param rowKey         行健
     * @param dataPackObject 车辆数据
     * @param receiveTime    网关接收时间
     */
    protected void saveDataPackObject(String rowKey, DataPackObject dataPackObject, Date receiveTime) {
        // 打印日志
        s_logger.debug("saveDataPackObject: {}, {}", rowKey, dataPackObject);
        //获取数据类型，以便打印激活数据日志
        String dataType = DataPackObjectUtil.getDataType(dataPackObject);
        try {
            // 保存数据
            _host.saveDataPackObject(rowKey, dataPackObject);
            s_logger.debug("Save {} success!", rowKey);

        } catch (Exception e) {
            e.printStackTrace();
            s_logger.error("Save failed: {}, {}", rowKey, e.getMessage());
        }
    }

    /**
     * 保存数据
     *
     * @param vin                车架号
     * @param dataPackTargetList 数据列表(同一个DataPack解出的)
     * @param receiveTime        数据接收时间（gather服务器接收时间，非设备采集时间）
     */
    private Map<String, DataPackObject> saveDataPacks(String vin, List<DataPackTarget> dataPackTargetList, Date receiveTime) {
        // 处理采集时间，生成rowKey
        Map<String, DataPackObject> mapDataPackObjects = checkDetectionTimeAndDealCachePool(dataPackTargetList, receiveTime, vin);
        mapDataPackObjects.forEach((key, value) -> {
            // 保持数据到BigTable
            saveDataPackObject(key, value, receiveTime);
        });

        return mapDataPackObjects;
    }

    /**
     * 分发数据包
     *
     * @param dataPack        原始数据包
     * @param dataPackObjects 车辆数据列表(同一个DataPack解出的)
     */
    private void dispatchDataPacks(DataPack dataPack, Map<String, DataPackObject> dataPackObjects) {
        // 分发到转存器
        dataPackObjects.forEach((key, object) -> {
            if (object instanceof DataPackLogIn) {
                // 分发车辆登录数据
                cacheManager.lpush(Constants.CacheNamespaceKey.CACHE_MESSAGE_QUEUE, key);

            } else if (object instanceof DataPackRsaKeyRequest) {
                // 分发公钥更新请求数据
                cacheManager.lpush(Constants.CacheNamespaceKey.CACHE_MESSAGE_QUEUE, key);

            } else if (object instanceof DataPackRsaKeyCompleted) {
                // 分发公钥更新完成数据
                cacheManager.lpush(Constants.CacheNamespaceKey.CACHE_MESSAGE_QUEUE, key);

            } else if (object instanceof DataPackAlarm) {
                // 分发车辆报警数据
                cacheManager.lpush(Constants.CacheNamespaceKey.CACHE_MESSAGE_QUEUE, key);

            } else if (object instanceof DataPackFault) {
                // 分发车辆故障数据
                cacheManager.lpush(Constants.CacheNamespaceKey.CACHE_MESSAGE_QUEUE, key);

            } else if (object instanceof DataPackOtaCompleted) {
                // 分发OTA升级完成数据
                cacheManager.lpush(Constants.CacheNamespaceKey.CACHE_MESSAGE_QUEUE, key);

            } else if (object instanceof DataPackSettingCompleted) {
                // 分发T-BOX参数设置完成数据
                cacheManager.lpush(Constants.CacheNamespaceKey.CACHE_MESSAGE_QUEUE, key);

            } else if (object instanceof DataPackEcallData) {
                // 分发车辆ecall数据
                cacheManager.lpush(Constants.CacheNamespaceKey.CACHE_MESSAGE_QUEUE, key);

            } else if (object instanceof DataPackEcallEvent) {
                // 分发车辆ecall事件日志数据
                cacheManager.lpush(Constants.CacheNamespaceKey.CACHE_MESSAGE_QUEUE, key);

            } else if (object instanceof DataPackAlarmSettingCompleted) {
                // 分发T-BOX报警参数设置完成数据
                cacheManager.lpush(Constants.CacheNamespaceKey.CACHE_MESSAGE_QUEUE, key);

            } else if (object instanceof DataPackPosition) {
                // 缓存车辆最新位置数据，方便聚合点计算
                DataPackPosition dataPackPosition = (DataPackPosition) object;
                if (null != dataPackPosition && null != dataPackPosition.getLongitude() && null != dataPackPosition.getLatitude()) {
                    // 判断是否为正常的位置数据
                    if (!(0 == dataPackPosition.getLongitude() && 0 == dataPackPosition.getLatitude())) {
                        // GEO结构：vin = (longitude, latitude)
                        cacheManager.gset(Constants.CacheNamespaceKey.CACHE_VEHICLE_GEO, dataPackPosition.getVin(), dataPackPosition.getLongitude(), dataPackPosition.getLatitude());
                        // GEO扩展信息：vin = json(DataPackPosition)
                        cacheManager.hset(Constants.CacheNamespaceKey.CACHE_VEHICLE_GEO_EXTEND_HASH, dataPackPosition.getVin(), GsonFactory.newInstance().createGson().toJson(dataPackPosition));
                    }
                }
            }
        });

        // 分发到国标地标平台
        if (null != _host.getGbPushMQ() || null != _host.getDbPushMQ()) {
            try {
                // 构建消息
                MQMsg mqMsg = new MQMsg(dataPack.getMark(), dataPack.serializeToBytes());
                byte[] dataBytes = GsonFactory.newInstance().createGson().toJson(mqMsg).getBytes(Constants.DEFAULT_CHARSET);

                // 分发到国标平台
                if (null != _host.getGbPushMQ()) {
                    _host.getGbPushMQ().post(_host.getGbPushTopic(), dataBytes);
                }

                // 分发到地标
                if (null != _host.getDbPushMQ()) {
                    _host.getDbPushMQ().post(_host.getDbPushTopic(), dataBytes);
                }

            } catch (UnsupportedEncodingException e) {
                s_logger.debug("Unsupported encoding {}.", Constants.DEFAULT_CHARSET);
            }
        }
    }

    /**
     * 缓存管理器
     */
    private ICacheManager cacheManager;

    /**
     * 设置缓存管理器
     *
     * @param cacheManager
     */
    public void setCacheManager(ICacheManager cacheManager) {
        this.cacheManager = cacheManager;
    }

    /**
     * 处理数据缓存关系
     *
     * @param dataPackObject datapack对象
     */
    private void operationCachePool(DataPackObject dataPackObject) {
        // 查询设备号和VIN码
        String deviceId = dataPackObject.getDeviceId();
        String vin = dataPackObject.getVin();

        // 根据设备号获取VIN码 <Redis中获取>
        if (null == vin) {
            vin = cacheManager.get(deviceId);
        }

        /**
         * 处理数据心跳包<br>
         *     device-heartbeat:  vin  --  {type, time}    单个车的心跳信息<br>
         *     device-online:  vin --  time    设置ttl=30s，过期自动删除    查询这个ns的vins就能知道"在线总数"<br>
         *     device-offline:   vin -- time   放离线车辆vin与离线时间   查询这个ns的vins就能知道"离线总数"<br>
         * "异常车辆"的总数：total = len(device-heartbeat) - len(device-online) - len(device-offline)
         * set(vin) = set(device-heartbeat:vin) - set(device-online:vin) - set(device-offline:vin)
         */
        int type = Constants.HeartbeatType.NORMAL;
        Date time = dataPackObject.getReceiveTime();

//        String timeStr = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(time);
        String timeStr = DateFormatUtils.format(time, "yyyy-MM-dd HH:mm:ss");
        if (dataPackObject instanceof DataPackLogIn || dataPackObject instanceof DataPackLogOut) {
            // VIN与设备号建立关系 （永久）
            cacheManager.hset(Constants.CacheNamespaceKey.CACHE_VEHICLE_VIN_HASH, vin, deviceId);

            // 设备号与VIN码建立关系 （永久）
            cacheManager.hset(Constants.CacheNamespaceKey.CACHE_DEVICE_ID_HASH, deviceId, vin);

            // 判断登陆类型
            Integer loginType = null;

            // 离线车辆关系 （永久） 在线与离线互斥
            if (dataPackObject instanceof DataPackLogIn) {
                DataPackLogIn dataPackLogIn = (DataPackLogIn) dataPackObject;
                loginType = dataPackLogIn.getLoginType();
            } else if (dataPackObject instanceof DataPackLogOut) {
                DataPackLogOut dataPackLogOut = (DataPackLogOut) dataPackObject;
                loginType = dataPackLogOut.getLoginType();
            }

            if (null != loginType) {
                type = loginType == 0 ? Constants.HeartbeatType.LOGIN : Constants.HeartbeatType.LOGOUT;
                if (type == Constants.HeartbeatType.LOGIN) {
                    cacheManager.hdelete(Constants.CacheNamespaceKey.CACHE_VEHICLE_OFFLINE_HASH, vin);
                } else if (type == Constants.HeartbeatType.LOGOUT) {
                    //车辆离线
                    cacheManager.hset(Constants.CacheNamespaceKey.CACHE_VEHICLE_OFFLINE_HASH, vin, timeStr);
                    cacheManager.delete(Constants.CacheNamespaceKey.CACHE_NS_VEHICLE_ONLINE + vin);
                }
            }
        }

        // 构建map数据
        Map<String, Object> map = new HashMap<>();
        map.put(Constants.HeartbeatDataMapKey.TYPE, type);
        map.put(Constants.HeartbeatDataMapKey.TIME, timeStr);

        if (!(dataPackObject instanceof DataPackLogOut) || !(dataPackObject instanceof DataPackActivation)) {
            //在线车辆关系 （30S）
            cacheManager.set(Constants.CacheNamespaceKey.CACHE_NS_VEHICLE_ONLINE + vin, timeStr, Constants.DEFAULT_HEARTBEAT_TIMEOUT);
        }

        //连线过的车辆关系 （永久）-- 所有数据均为心跳数据
        cacheManager.hset(Constants.CacheNamespaceKey.CACHE_VEHICLE_HEARTBEAT_HASH, vin, GsonFactory.newInstance().createGson().toJson(map));
    }

    /**
     * 判断是否是激活报文
     *
     * @param dataPackBytes
     * @return
     */
    private boolean isActivateData(byte[] dataPackBytes) {
        if (null != dataPackBytes && dataPackBytes.length > 4 && (dataPackBytes[4] & 0xFF) == 0x12) {
            return true;
        }
        return false;
    }

    /**
     * 判断是否是故障报文
     */
    private boolean isFaultData(byte[] dataPackBytes) {
        if (null != dataPackBytes && dataPackBytes.length > 4 && ((dataPackBytes[4] & 0xFF) == 0x0C || (dataPackBytes[4] & 0xFF) == 0x27)) {
            return true;
        }
        return false;
    }
}
