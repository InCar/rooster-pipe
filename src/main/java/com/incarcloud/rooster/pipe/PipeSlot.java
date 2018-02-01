package com.incarcloud.rooster.pipe;

import com.alibaba.fastjson.JSON;
import com.incarcloud.rooster.cache.ICacheManager;
import com.incarcloud.rooster.datapack.*;
import com.incarcloud.rooster.mq.IBigMQ;
import com.incarcloud.rooster.mq.MQMsg;
import com.incarcloud.rooster.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

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
    private static final int BATCH_RECEIVE_SIZE = 10;

    /**
     * 接收数据线程数
     */
    private static final int RECEIVE_DATA_THREAD_COUNT = 2;

    /**
     * 执行定时监控运行状态的线程池
     */
    private ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1);

    /**
     * 监控数据：截止现在从消息队列取出的消息数量
     */
    private volatile AtomicInteger receiveFromMqDataCount = new AtomicInteger(0);

    /**
     * 监控数据：截止现在保存到bigtable的数据条数
     */
    private volatile AtomicInteger saveToBigtableDataCount = new AtomicInteger(0);

    /**
     * 监控数据：截止现在保存到bigtable失败的数据条数
     */
    private volatile AtomicInteger saveToBigtableFailedDataCount = new AtomicInteger(0);

    /**
     * 上次统计的值-取出的消息数量
     */
    private volatile int lastReceiveFromMqDataCount = 0;

    /**
     * 上次统计的值-保存到bigtable的数据条数
     */
    private volatile int lastSaveToBigtableDataCount = 0;

    /**
     * 上次统计的值-保存到bigtable失败的数据条数
     */
    private volatile int lastSaveToBigtableFailedDataCount = 0;

    /**
     * 统计间隔时间（秒）
     */
    private int period = 20;

    /**
     * 名称
     */
    private String name;

    /**
     * slot是否继续工作
     */
    private volatile boolean isRunning = false;

    /**
     * slot 工作线程组(后期可能需要管理线程)
     */
    private ThreadGroup workThreadGroup = new ThreadGroup(name + "workThreadGroup");

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

        // 开个线程防止阻塞
        for (int i = 0; i < RECEIVE_DATA_THREAD_COUNT; i++) {
            Thread workThread = new Thread(workThreadGroup, new PipeSlotReceiveDateProcess(name + "-PipeSlotProcess-" + i, _host.getReceiveDataMQ()));
            workThread.start();
        }

        // 间隔一定时间监控运行状况
        scheduledExecutorService.scheduleAtFixedRate(new Runnable() {

            public void run() {
                // 取出的消息数量
                int _receiveFromMqDataCount = receiveFromMqDataCount.get();
                int newReceiveFromMqDataCount = _receiveFromMqDataCount - lastReceiveFromMqDataCount;
                lastReceiveFromMqDataCount = _receiveFromMqDataCount;

                // 保存到bigtable的数据条数
                int _saveToBigtableDataCount = saveToBigtableDataCount.get();
                int newSaveToBigtableDataCount = _saveToBigtableDataCount - lastSaveToBigtableDataCount;
                lastSaveToBigtableDataCount = _saveToBigtableDataCount;

                // 保存到bigtable失败的数据条数
                int _saveToBigtableFailedDataCount = saveToBigtableFailedDataCount.get();
                int newSaveToBigtableFailedDataCount = _saveToBigtableFailedDataCount - lastSaveToBigtableFailedDataCount;
                lastSaveToBigtableFailedDataCount = _saveToBigtableFailedDataCount;

                // 记录日志
                s_logger.info("{} in last {} s, total {} msg receive from mq, total {} saved to bigtable, total {} failed save to bigtable!!!", PipeSlot.this.name, period, newReceiveFromMqDataCount, newSaveToBigtableDataCount, newSaveToBigtableFailedDataCount);
            }
        }, period, period, TimeUnit.SECONDS);

        s_logger.info(name + " start success!!!");
    }

    /**
     * 停止
     */
    public void stop() {
        isRunning = false;// 等待线程自己结束
        scheduledExecutorService.shutdownNow();
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

        @Override
        public void run() {

            while (isRunning) {
                List<byte[]> msgList = receiveDataMQ.batchReceive(_host.getReceiveDataTopic(), BATCH_RECEIVE_SIZE);

                if (null == msgList || 0 == msgList.size()) {
                    s_logger.debug("{} receive no  message !!!", name);
                    try {
                        Thread.sleep(3000);
                    } catch (InterruptedException e) {
                    }
                    continue;
                } else {// 监控数据
                    receiveFromMqDataCount.getAndAdd(msgList.size());
                }

                for (byte[] b : msgList) {
                    DataPack dp = null;

                    try {
                        String json = new String(b);
                        MQMsg m = GsonFactory.newInstance().createGson().fromJson(json, MQMsg.class);
                        dp = DataPack.deserializeFromBytes(m.getData());
                        s_logger.debug("DataPack: {}", dp.toString());

                        IDataParser dataParser = DataParserManager.getDataParser(dp.getProtocol());
                        if (null == dataParser) {
                            s_logger.error("Not support {}!!!", dp.getProtocol());
                            continue;
                        }

                        // 第二步解析
                        List<DataPackTarget> dataPackTargetList = dataParser.extractBody(dp);// 同一个DataPack解出的数据列表

                        if (null == dataPackTargetList || 0 == dataPackTargetList.size()) {
                            s_logger.info("extractBody: null, dataPackTargetList: {}, DataPack: {}", m, dp);
                            continue;
                        }

                        String vin = null;// 获取消息中传过来的vin
                        if (m.getMark().contains("|")) {
                            vin = m.getMark().substring(m.getMark().indexOf("|") + 1);
                        }

                        if (!StringUtil.isBlank(vin)) {// 补充vin码
                            for (DataPackTarget t : dataPackTargetList) {
                                t.getDataPackObject().setVin(vin);
                            }
                        }

                        for (DataPackTarget t : dataPackTargetList) {
                            s_logger.info("--> {}", t.toString());
                        }

                        // 保存数据
                        saveDataPacks(dataPackTargetList, dp.getReceiveTime());
                        dispatchDataPacks(dp, dataPackTargetList);// 分发

                    } catch (Exception e) {
                        e.printStackTrace();
                        s_logger.error("deal with msg error, {}, \n{}", new String(b), e.getMessage());
                    } finally {
                        if (null != dp) {
                            dp.freeBuf();
                        }
                    }
                }
            }

            // 停止后释放连接
            receiveDataMQ.releaseCurrentConn(_host.getReceiveDataTopic());
        }
    }

    /**
     * 从数据对象数据列表(同一个DataPack解出的，vin应该都相同，可能有的没有vin码)获取vin码，无vin码则返回 设备id
     *
     * @param dataPackTargetList 数据对象数据列表(同一个DataPack解出的)
     * @return vin码
     */
    private String checkAndGetVin(List<DataPackTarget> dataPackTargetList) {
        String vin = null;
        Iterator<DataPackTarget> iterator = dataPackTargetList.iterator();

        // 处理无vin的数据
        while (iterator.hasNext()) {
            DataPackObject dataPackObject = iterator.next().getDataPackObject();
            String deviceId = dataPackObject.getDeviceId();
            String vin0 = dataPackObject.getVin();
            if (StringUtil.isBlank(vin0) && StringUtil.isBlank(deviceId)) {
                s_logger.error("Invalid data: no vin or deviceId");
                iterator.remove();
                continue;
            }

            // 没有vin码时候,设备ID代替vin码
            if (StringUtil.isBlank(vin0)) {
                s_logger.debug("No vin: {}", dataPackObject);
                vin = deviceId;
            } else {
                vin = vin0;
                break;
            }
        }

        return vin;
    }

    /**
     * 处理数据
     * 1.设置数据接收时间<br />
     * 2.对采集时间字段为空或无效的数据进行处理<br>
     * 3.生成rowkey，返回 rowkey -> DataPackObject
     *
     * @param dataPackTargetList 数据列表(同一个DataPack解出的)
     * @param receiveTime        数据接收时间（gather服务器接收时间，非设备采集时间）
     * @param vin                vin码
     * @return 待保存的数据
     */
    private Map<String, DataPackObject> treatDetectionAndGetDataPackObject(List<DataPackTarget> dataPackTargetList, Date receiveTime, String vin) {
        Map<String, DataPackObject> dataForSave = new HashMap<>();

        // 处理检测日期
        /**
         * 对于无采集时间或采集时间为非法时间，做如下处理：<br />
         * 1.该批数据（属于同一个包）含有位置数据 且位置数据带有合法采集时间，用位置数据的采集时间来重置所有包的采集时间；<br />
         * 2.该批数据（属于同一个包）不含位置数据 或 位置数据不带有合法采集时间，用接收时间重置所有包的采集时间。
         */
        Date receiveTime0 = receiveTime;
        for (DataPackTarget target : dataPackTargetList) {// 获取位置时间覆盖接收时间
            if (target.getDataPackObject() instanceof DataPackPosition) {
                DataPackPosition position = (DataPackPosition) target.getDataPackObject();
                if (DataPackObjectUtils.isLegalDetectionDate(position.getPositionTime())) {
                    receiveTime0 = position.getPositionTime();
                    break;
                }

            }
        }

        for (DataPackTarget target : dataPackTargetList) {
            // 1、校正非法采集时间
            DataPackObject packObject = target.getDataPackObject();
            packObject.setReceiveTime(receiveTime);

            // 2、采集时间被接收时间重置
            String timeStr = null;
            if (DataPackObjectUtils.checkAndResetIlllegalDetectionDate(packObject, receiveTime0)) {
                timeStr = DataPackObjectUtils.convertDetectionDateToString(packObject.getDetectionTime()) + "N";// N表示设备未上传数据采集时间，系统自动加上采集时间
            } else {
                timeStr = DataPackObjectUtils.convertDetectionDateToString(packObject.getDetectionTime());
            }

            // 3、保存
            String dataType = DataPackObjectUtils.getDataType(packObject);// 数据类型
            String rowKey = RowKeyUtil.makeRowKey(vin, dataType, timeStr);
            dataForSave.put(rowKey, packObject);

            // 4、监控车辆状态信息
            operationRedis(packObject);
        }

        return dataForSave;
    }

    /**
     * 保存vin码
     *
     * @param vin
     */
    protected void saveVin(String vin) {
        try {
            _host.saveVin(vin);
        } catch (Exception e) {
            s_logger.error("Save vin error, vin={}, {}", vin, e.getMessage());
        }
    }

    /**
     * 保存数据
     *
     * @param rowKey         bigtable主键列值
     * @param dataPackObject 数据对象
     * @param receiveTime    数据接收时间（gather服务器接收时间，非设备采集时间），二级索引用接收时间时间生成便于同步数据
     */
    protected void saveDataPackObject(String rowKey, DataPackObject dataPackObject, Date receiveTime) {
        s_logger.debug("saveDataPackObject: {}, {}", rowKey, dataPackObject);
        try {
            _host.saveDataPackObject(rowKey, dataPackObject, receiveTime);
            saveToBigtableDataCount.incrementAndGet();
            s_logger.debug("Save success: ", rowKey);
        } catch (Exception e) {
            e.printStackTrace();
            s_logger.error("Save failed: {}, {}", rowKey, e.getMessage());
            saveToBigtableFailedDataCount.incrementAndGet();
        }
    }

    /**
     * 保存数据
     *
     * @param dataPackTargetList 数据列表(同一个DataPack解出的)
     * @param receiveTime        数据接收时间（gather服务器接收时间，非设备采集时间）
     */
    private void saveDataPacks(List<DataPackTarget> dataPackTargetList, Date receiveTime) {
        // 获取vin码
        String vin = checkAndGetVin(dataPackTargetList);

        // 保存vin码
        saveVin(vin);

        // 处理采集时间,生成rowkey
        Map<String, DataPackObject> dataForSave = treatDetectionAndGetDataPackObject(dataPackTargetList, receiveTime, vin);
        for (Map.Entry<String, DataPackObject> data : dataForSave.entrySet()) {
            saveDataPackObject(data.getKey(), data.getValue(), receiveTime);
        }

    }

    /**
     * 分发数据包
     *
     * @param dp                 原始数据包
     * @param dataPackTargetList 数据列表(同一个DataPack解出的)
     */
    private void dispatchDataPacks(DataPack dp, List<DataPackTarget> dataPackTargetList) {
        // 分发到国标
        if (null != _host.getGbPushMQ()) {
            IBigMQ gbPushMQ = _host.getGbPushMQ();

            try {
                MQMsg mqMsg = new MQMsg(dp.getMark(), dp.serializeToBytes());
                byte[] bytes = GsonFactory.newInstance().createGson().toJson(mqMsg).getBytes();
                gbPushMQ.post(_host.getGbPushTopic(), bytes);
            } catch (UnsupportedEncodingException e) {
                s_logger.debug("Unsupported encoding utf-8.");
            }
        }

        // TODO 分发到地标
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
     * 数据处理存入Redis
     *
     * @param packObject
     */
    private void operationRedis(DataPackObject packObject) {
        // 查询设备号和VIN码
        String deviceCode = packObject.getDeviceId();
        String vin = packObject.getVin();

        // 根据设备号获取VIN码 <Redis中获取>
        if (null == vin) {
            vin = cacheManager.get(deviceCode);
        }

        // 处理数据心跳包
        int type = Constant.HeartbeatType.NORMAL;
        Date time = packObject.getDetectionTime();
        String timeStr = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(time);
        if (packObject instanceof DataPackLogInOut) {
            //VIN与设备号建立关系 （永久）
            cacheManager.set(Constant.RedisNamespace.REDIS_NS_VEHICLE_VIN + vin, deviceCode);

            //设备号与VIN码建立关系 （永久）
            cacheManager.set(Constant.RedisNamespace.REDIS_NS_DEVICE_CODE + deviceCode, vin);

            //离线车辆关系 （永久） 在线与离线互斥
            DataPackLogInOut dataPackLogInOut = (DataPackLogInOut) packObject;

            // 判断登陆类型
            Integer loginType = dataPackLogInOut.getLoginType();
            if (null != loginType) {
                type = loginType == 0 ? Constant.HeartbeatType.LOGIN : Constant.HeartbeatType.LOGOUT;
                if (type == Constant.HeartbeatType.LOGIN) {
                    //在线车辆关系 （30S）
                    cacheManager.set(Constant.RedisNamespace.REDIS_NS_DEVICE_ONLINE + vin, timeStr, Constant.TIME_OUT);
                    cacheManager.delete(Constant.RedisNamespace.REDIS_NS_DEVICE_OFFLINE + vin);
                } else if (type == Constant.HeartbeatType.LOGOUT) {
                    //车辆离线
                    cacheManager.set(Constant.RedisNamespace.REDIS_NS_DEVICE_OFFLINE + vin, timeStr);
                }
            }
        }

        // 构建map数据
        Map<String, Object> map = new HashMap<>();
        map.put(Constant.HeartbeatDataMapKey.TYPE, type);
        map.put(Constant.HeartbeatDataMapKey.TIME, timeStr);

        //连线过的车辆关系 （永久）-- 所有数据均为心跳数据
        cacheManager.set(Constant.RedisNamespace.REDIS_NS_DEVICE_HEARTBEAT + vin, JSON.toJSON(map).toString());
    }
}
