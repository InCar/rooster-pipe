package com.incarcloud.rooster.pipe;

import com.incarcloud.rooster.cache.ICacheManager;
import com.incarcloud.rooster.datapack.*;
import com.incarcloud.rooster.mq.IBigMQ;
import com.incarcloud.rooster.mq.MQMsg;
import com.incarcloud.rooster.share.Constants;
import com.incarcloud.rooster.util.DataPackObjectUtils;
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
     * 接收数据线程数
     */
    private static final int RECEIVE_DATA_THREAD_COUNT = 1;

//    /**
//     * 执行定时监控运行状态的线程池
//     */
//    private ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1);

//    /**
//     * 监控数据：截止现在从消息队列取出的消息数量
//     */
//    private volatile AtomicInteger receiveFromMqDataCount = new AtomicInteger(0);
//
//    /**
//     * 监控数据：截止现在保存到bigtable的数据条数
//     */
//    private volatile AtomicInteger saveToBigtableDataCount = new AtomicInteger(0);
//
//    /**
//     * 监控数据：截止现在保存到bigtable失败的数据条数
//     */
//    private volatile AtomicInteger saveToBigtableFailedDataCount = new AtomicInteger(0);

//    /**
//     * 上次统计的值-取出的消息数量
//     */
//    private volatile int lastReceiveFromMqDataCount = 0;
//
//    /**
//     * 上次统计的值-保存到bigtable的数据条数
//     */
//    private volatile int lastSaveToBigtableDataCount = 0;
//
//    /**
//     * 上次统计的值-保存到bigtable失败的数据条数
//     */
//    private volatile int lastSaveToBigtableFailedDataCount = 0;

//    /**
//     * 统计间隔时间（秒）
//     */
//    private int period = 20;

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
//        for (int i = 0; i < RECEIVE_DATA_THREAD_COUNT; i++) {
//            Thread workThread = new Thread(workThreadGroup, new PipeSlotReceiveDateProcess(name + "-PipeSlotProcess-" + i, _host.getReceiveDataMQ()));
//            workThread.start();
//        }

        Thread workThread = new Thread(new PipeSlotReceiveDateProcess(name + "-PipeSlotProcess-" + 0, _host.getReceiveDataMQ())) ;
        workThread.start();

//        // 间隔一定时间监控运行状况
//        scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
//
//            public void run() {
//                // 取出的消息数量
//                int _receiveFromMqDataCount = receiveFromMqDataCount.get();
//                int newReceiveFromMqDataCount = _receiveFromMqDataCount - lastReceiveFromMqDataCount;
//                lastReceiveFromMqDataCount = _receiveFromMqDataCount;
//
//                // 保存到bigtable的数据条数
//                int _saveToBigtableDataCount = saveToBigtableDataCount.get();
//                int newSaveToBigtableDataCount = _saveToBigtableDataCount - lastSaveToBigtableDataCount;
//                lastSaveToBigtableDataCount = _saveToBigtableDataCount;
//
//                // 保存到bigtable失败的数据条数
//                int _saveToBigtableFailedDataCount = saveToBigtableFailedDataCount.get();
//                int newSaveToBigtableFailedDataCount = _saveToBigtableFailedDataCount - lastSaveToBigtableFailedDataCount;
//                lastSaveToBigtableFailedDataCount = _saveToBigtableFailedDataCount;
//
//                // 记录日志
//                s_logger.info("{} in last {} s, total {} msg receive from mq, total {} saved to bigtable, total {} failed save to bigtable!!!", PipeSlot.this.name, period, newReceiveFromMqDataCount, newSaveToBigtableDataCount, newSaveToBigtableFailedDataCount);
//            }
//        }, period, period, TimeUnit.SECONDS);

        s_logger.info(name + " start success!!!");
    }

    /**
     * 停止
     */
    public void stop() {
        isRunning = false;// 等待线程自己结束
//        scheduledExecutorService.shutdownNow();
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

            // 开启10个线程消费队列消息
            for (int i = 0; i < 10 ; i++) {
                new Thread(()->dealQueueMsg()).start();
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
                if (queue.size() > 2000){
                    s_logger.info("queue msg accumulation, waiting 1s ...");
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
//                else {
//                    // 监控数据
//                    receiveFromMqDataCount.getAndAdd(msgList.size());
//                }
                // 存放无边界消息队列
                queue.add(msgList) ;
            }

            // 停止后释放连接
            receiveDataMQ.releaseCurrentConn(_host.getReceiveDataTopic());
        }

        /**
         * 消费队列消息
         */
        private void dealQueueMsg(){
            while(true){
                long start = System.currentTimeMillis() ;
                if (queue.size() > 0){
                    List<byte[]> msgList = queue.poll() ;
                    if (null == msgList) {
                        continue;
                    }
                    dealMQMsg(msgList) ;
                    s_logger.info("deal one group time {}",(System.currentTimeMillis()-start));
                }
            }
        }

        /**
         * 处理MQ消息
         * @param msgList
         */
        private void dealMQMsg(List<byte[]> msgList){
            if (null == msgList || msgList.size() == 0){
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
                        s_logger.error("Not support {}!!!", dp.getProtocol());
                        continue;
                    }

                    // 调用解析器解析完整报文
                    List<DataPackTarget> dataPackTargetList = dataParser.extractBody(dp);// 同一个DataPack解出的数据列表
                    if (null == dataPackTargetList || 0 == dataPackTargetList.size()) {
                        s_logger.info("extractBody: null, dataPackTargetList: {}, DataPack: {}", m, dp);
                        continue;
                    }

                    // 获取消息中传过来的deviceId，如果VIN没有默认deviceId
                    String vin = null;
                    if (m.getMark().contains("|")) {
                        vin = m.getMark().substring(m.getMark().indexOf("|") + 1);
                    }

                    // 从缓存提取VIN信息
                    String cacheVin = cacheManager.hget(Constants.CacheNamespaceKey.CACHE_DEVICE_ID_HASH , vin);
                    if(StringUtils.isNotBlank(cacheVin)) {
                        vin = cacheVin;
                    }

                    // 完善DataPack车辆VIN
                    if (StringUtils.isNotBlank(vin)) {
                        for (DataPackTarget dataPackTarget : dataPackTargetList) {
                            // 设置VIN信息
                            dataPackTarget.getDataPackObject().setVin(vin);

                            // TODO 设置timestamp到id，云平台测试完毕后删除
                            dataPackTarget.getDataPackObject().setId("" + dp.getGatherTime().getTime());
                        }
                    }

//                    // 记录日志
//                    for (DataPackTarget t : dataPackTargetList) {
//                        s_logger.info("--> {}", t.toString());
//                    }

                    // 保存数据
                    saveDataPacks(dataPackTargetList, dp.getReceiveTime());

                    // 分发数据
                    dispatchDataPacks(dp, dataPackTargetList);

                } catch (Exception e) {
                    e.printStackTrace();
                    s_logger.error("Deal with msg error, {}, \n{}, \n{}", new String(msg), ExceptionUtils.getMessage(e),e.getMessage());
                } finally {
                    if (null != dp) {
                        dp.freeBuf();
                    }
                }
            }
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
            if (StringUtils.isBlank(vin0) && StringUtils.isBlank(deviceId)) {
                s_logger.error("Invalid data: no vin or deviceId");
                iterator.remove();
                continue;
            }

            // 没有vin码时候,设备ID代替vin码
            if (StringUtils.isBlank(vin0)) {
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
        // 初始化
        Map<String, DataPackObject> dataForSave = new HashMap<>();

        // 处理检测日期
        /**
         * 对于无采集时间或采集时间为非法时间，做如下处理：<br />
         * 1.该批数据（属于同一个包）含有位置数据 且位置数据带有合法采集时间，用位置数据的采集时间来重置所有包的采集时间；<br />
         * 2.该批数据（属于同一个包）不含位置数据 或 位置数据不带有合法采集时间，用接收时间重置所有包的采集时间。
         */
        Date receiveTime0 = receiveTime;
        for (DataPackTarget target : dataPackTargetList) {
            // 获取位置时间覆盖接收时间
            if (target.getDataPackObject() instanceof DataPackPosition) {
                DataPackPosition position = (DataPackPosition) target.getDataPackObject();
                if (DataPackObjectUtils.isLegalDetectionDate(position.getPositionTime())) {
                    receiveTime0 = position.getPositionTime();
                    break;
                }

            }
        }

        for (DataPackTarget target : dataPackTargetList) {
            // 1.校正非法采集时间
            DataPackObject packObject = target.getDataPackObject();
            packObject.setReceiveTime(receiveTime);

            // 2.采集时间被接收时间重置
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
            operationCahche(packObject);
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
//            saveToBigtableDataCount.incrementAndGet();
            s_logger.debug("Save success: ", rowKey);
        } catch (Exception e) {
            e.printStackTrace();
            s_logger.error("Save failed: {}, {}", rowKey, e.getMessage());
//            saveToBigtableFailedDataCount.incrementAndGet();
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
            // 保持数据到BigTable
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
     * 数据处理存入缓存
     *
     * @param packObject datapack对象
     */
    private void operationCahche(DataPackObject packObject) {
        // 查询设备号和VIN码
        String deviceId = packObject.getDeviceId();
        String vin = packObject.getVin();

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
        Date time = packObject.getDetectionTime();
        String timeStr = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(time);
        if (packObject instanceof DataPackLogIn || packObject instanceof DataPackLogOut) {
            //VIN与设备号建立关系 （永久）
            cacheManager.hset(Constants.CacheNamespaceKey.CACHE_VEHICLE_VIN_HASH , vin, deviceId);

            //设备号与VIN码建立关系 （永久）
            cacheManager.hset(Constants.CacheNamespaceKey.CACHE_DEVICE_ID_HASH , deviceId, vin);

            // 判断登陆类型
            Integer loginType = null ;

            //离线车辆关系 （永久） 在线与离线互斥
            if (packObject instanceof DataPackLogIn){
                DataPackLogIn dataPackLogIn = (DataPackLogIn) packObject;
                loginType = dataPackLogIn.getLoginType();
            }else if(packObject instanceof DataPackLogOut){
                DataPackLogOut dataPackLogOut = (DataPackLogOut) packObject;
                loginType = dataPackLogOut.getLoginType();
            }

            if (null != loginType) {
                type = loginType == 0 ? Constants.HeartbeatType.LOGIN : Constants.HeartbeatType.LOGOUT;
                if (type == Constants.HeartbeatType.LOGIN) {
                    cacheManager.hdelete(Constants.CacheNamespaceKey.CACHE_VEHICLE_OFFLINE_HASH , vin);
                } else if (type == Constants.HeartbeatType.LOGOUT) {
                    //车辆离线
                    cacheManager.hset(Constants.CacheNamespaceKey.CACHE_VEHICLE_OFFLINE_HASH , vin, timeStr);
                    cacheManager.delete(Constants.CacheNamespaceKey.CACHE_NS_VEHICLE_ONLINE + vin);
                }
            }
        }

        // 构建map数据
        Map<String, Object> map = new HashMap<>();
        map.put(Constants.HeartbeatDataMapKey.TYPE, type);
        map.put(Constants.HeartbeatDataMapKey.TIME, timeStr);

        if (!(packObject instanceof DataPackLogOut) || !(packObject instanceof DataPackActivation)){
            //在线车辆关系 （30S）
            cacheManager.set(Constants.CacheNamespaceKey.CACHE_NS_VEHICLE_ONLINE + vin, timeStr, Constants.DEFAULT_HEARTBEAT_TIMEOUT);
        }

        //连线过的车辆关系 （永久）-- 所有数据均为心跳数据
        cacheManager.hset(Constants.CacheNamespaceKey.CACHE_VEHICLE_HEARTBEAT_HASH , vin, GsonFactory.newInstance().createGson().toJson(map));
    }
}
