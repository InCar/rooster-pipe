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
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * @author Xiong Guanghua
 * @Description: 管道槽, 一个管道槽对应一个队列
 * @date 2017年6月2日 下午3:55:17
 */
public class PipeSlot {

    /**
     * Logger
     */
    private static Logger s_logger = LoggerFactory.getLogger(PipeSlot.class);

    /**
     * 一批次接受消息的数量
     */
    private static final int BATCH_RECEIVE_SIZE = 200;

    /**
     * 初始化处理队列线程数量
     */
    private static final int DEAL_QUEUE_THREAD = 50;

    /**
     * 名称
     */
    private String name;

    /**
     * slot是否继续工作
     */
    private volatile boolean isRunning = false;

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
        isRunning = true;

        Thread workThread = new Thread(new PipeSlotReceiveDateProcess(name + "-PipeSlotProcess-" + 0, _host.getReceiveDataMQ()));
        workThread.start();

        s_logger.info(name + " start success!!!");
    }

    /**
     * 停止
     */
    public void stop() {
        isRunning = false;// 等待线程自己结束
    }

    /**
     * slot主要工作线程
     */
    private class PipeSlotReceiveDateProcess implements Runnable {

        /**
         * 线程名称
         */
        private String name;
        private IBigMQ receiveDataMQ;

        public PipeSlotReceiveDateProcess(String name, IBigMQ receiveDataMQ) {
            this.name = name;
            this.receiveDataMQ = receiveDataMQ;
        }

        @Override
        public String toString() {
            return name;
        }

        /**
         * 并发队列-无界非阻塞队列
         */
        private Queue<List<byte[]>> queue = new ConcurrentLinkedQueue<>();

        @Override
        public void run() {

            // 开启线程消费队列消息
            for (int i = 0; i < DEAL_QUEUE_THREAD; i++) {
                new Thread(() -> dealQueueMsg()).start();
            }

            // 只获取MQ消息放入队列，不进行别的操作，为了加快消费MQ消息
            while (isRunning) {
                // 消费MQ消息
                List<byte[]> msgList = receiveDataMQ.batchReceive(_host.getReceiveDataTopic(), BATCH_RECEIVE_SIZE);

                // 如果消息列表为空，等待1000毫秒
                if (null == msgList || 0 == msgList.size()) {
                    // 打印消息队列名称
                    s_logger.debug("{} receive no message!!!", name);
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        s_logger.error(ExceptionUtils.getMessage(e));
                    }
                    continue;
                }

                // 如果队列消息大于2000没有消息，则等待，一般情况不会达到
                if (queue.size() > 2000) {
                    s_logger.info("queue msg accumulation, waiting 1s ...");
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }

                // 存放无边界消息队列
                queue.add(msgList);

                /**
                 * 默认从MQ主动获取是50条数据
                 * 如果数据条数小于50条数据时，则说明MQ数据比较少
                 * 等待1S降低性能
                 */
                if (msgList.size() < 50) {
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }

            // 停止后释放连接
            receiveDataMQ.releaseCurrentConn(_host.getReceiveDataTopic());
        }

        /**
         * 消费队列消息
         */
        private void dealQueueMsg() {
            while (true) {
                if (queue.size() > 0) {
                    List<byte[]> msgList = queue.poll();
                    if (null == msgList) {
                        continue;
                    }
                    dealMQMsg(msgList);
                    /**
                     * 默认从MQ主动获取是50条数据
                     * 如果数据条数小于50条数据时，则说明MQ数据比较少
                     * 等待1S降低性能
                     */
                    if (msgList.size() < 50) {
                        try {
                            Thread.sleep(1000);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                } else {
                    /**
                     * 没有数据则等待1S再处理
                     */
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }

        /**
         * 处理MQ消息
         *
         * @param msgList 消息列表
         */
        private void dealMQMsg(List<byte[]> msgList) {
            if (null == msgList || msgList.size() == 0) {
                return;
            }
            // 处理消息
            for (byte[] msg : msgList) {
                DataPack dp = null;

                try {
                    // string转到datapack
                    String json = new String(msg);
                    MQMsg m = GsonFactory.newInstance().createGson().fromJson(json, MQMsg.class);
                    dp = DataPack.deserializeFromBytes(m.getData());
                    s_logger.debug("DataPack: {}", dp.toString());

                    // 获得解析器
                    IDataParser dataParser = DataParserManager.getDataParser(dp.getProtocol());
                    if (null == dataParser) {
                        s_logger.error("Not support: {}!!!", dp.getProtocol());
                        continue;
                    }

                    // 调用解析器解析完整报文
                    List<DataPackTarget> dataPackTargetList = dataParser.extractBody(dp);// 同一个DataPack解出的数据列表
                    if (null == dataPackTargetList || 0 == dataPackTargetList.size()) {
                        s_logger.info("extractBody: null, dataPackTargetList: {}, DataPack: {}", m, dp);
                        continue;
                    }

                    // 获取消息中传过来的deviceId
                    String deviceId = m.getMark().split("\\|")[1];
                    if (StringUtils.isBlank(deviceId)) {
                        s_logger.error("Invalid data: no deviceId!", deviceId);
                        continue;
                    }

                    // 从缓存提取VIN信息
                    String vin = cacheManager.hget(Constants.CacheNamespaceKey.CACHE_DEVICE_ID_HASH, deviceId);
                    if (StringUtils.isBlank(vin)) {
                        s_logger.error("Invalid deviceId({}): no vin!", deviceId);
                        continue;
                    }

                    // 完善DataPack信息，主要是车架号
                    dataPackTargetList.forEach(object -> {
                        if (null != object.getDataPackObject()) {
                            object.getDataPackObject().setVin(vin);
                        }
                    });

                    // 永久保存数据到BigTable
                    Map<String, DataPackObject> mapDataPackObjects = saveDataPacks(vin, dataPackTargetList, dp.getReceiveTime());

                    // 分发数据
                    dispatchDataPacks(dp, mapDataPackObjects);

                } catch (Exception e) {
                    e.printStackTrace();
                    s_logger.error("Deal with msg error, {}, \n{}, \n{}", new String(msg), ExceptionUtils.getMessage(e), e.getMessage());
                } finally {
                    if (null != dp) {
                        dp.freeBuf();
                    }
                }
            }
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

            // 2.判断检测时间无效情况
            Date detectionTime = target.getDataPackObject().getDetectionTime();
            if (DataPackObjectUtil.isLegalDetectionDate(detectionTime)) {
                // 判断依据：比当前时间晚1个月或者早30分钟视为无效数据，主动丢弃
                s_logger.info("Legal detection date data: {}", DataPackObjectUtil.toJson(target.getDataPackObject()));
                continue;
            }

            // 3.创建rowkey和datapack关系
            String detectionTimeString = DataPackObjectUtil.convertDetectionTimeToString(detectionTime);
            String dataType = DataPackObjectUtil.getDataType(dataPackObject);// 数据类型
            String rowKey = RowKeyUtil.makeRowKey(vin, dataType, detectionTimeString);
            mapDataPackObjects.put(rowKey, dataPackObject);

            // 4、监控车辆状态信息
            operationCachePool(dataPackObject);
        }

        return mapDataPackObjects;
    }

    /**
     * 保存vin码
     *
     * @param vin 车架号
     */
    protected void saveVin(String vin) {
        try {
            // 保存车架号到HBase数据表
            _host.saveVin(vin);
        } catch (Exception e) {
            s_logger.error("Save vin error, vin={}, {}", vin, e.getMessage());
        }
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
        // 保存vin码
        if (StringUtils.isNotBlank(vin)) {
            saveVin(vin);
        }

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

            } else if (object instanceof DataPackPosition) {
                // 缓存车辆最新位置数据，方便聚合点计算
                DataPackPosition dataPackPosition = (DataPackPosition) dataPack;
                if (null != dataPackPosition && null != dataPackPosition.getLongitude() && null != dataPackPosition.getLatitude()) {
                    // 判断是否为正常的位置数据
                    if (0 < dataPackPosition.getLongitude() && 0 < dataPackPosition.getLatitude()) {
                        // GEO结构：vin = (longitude, latitude)
                        cacheManager.gset(Constants.CacheNamespaceKey.CACHE_VEHICLE_GEO, dataPackPosition.getVin(), dataPackPosition.getLongitude(), dataPackPosition.getLatitude());
                        // GEO扩展信息：vin = json(DataPackPosition)
                        cacheManager.hset(Constants.CacheNamespaceKey.CACHE_VEHICLE_GEO, dataPackPosition.getVin(), GsonFactory.newInstance().createGson().toJson(dataPackPosition));
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
        String timeStr = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(time);
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
}
