package com.incarcloud.pipe;

import com.incarcloud.rooster.datapack.*;
import com.incarcloud.rooster.mq.IBigMQ;
import com.incarcloud.rooster.mq.MQMsg;
import com.incarcloud.rooster.util.DataPackObjectUtils;
import com.incarcloud.rooster.util.RowKeyUtil;
import com.incarcloud.rooster.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
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
    private static Logger s_logger = LoggerFactory.getLogger(PipeSlot.class);


    /**
     * 缓存解析器对象避免Eden区频繁GC
     */
    private static Map<String, IDataParser> dataParserCache = new ConcurrentHashMap<>();


    /**
     * 一批次接受消息的数量
     */
    private static final int BATCH_RECEIVE_SIZE = 1;
    /**
     * 工作线程数
     */
    private static final int WORK_THREAD_COUNT = 1;


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
    private int period = 10;


    /**
     * 名称
     */
    private String name;

    /**
     * slot是否继续工作
     */
    private volatile boolean isRuning = false;


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
        s_logger.info(name + " start  receive  message !!");
        isRuning = true;
        //开个线程防止阻塞
        for (int i = 0; i < WORK_THREAD_COUNT; i++) {
            Thread workThread = new Thread(workThreadGroup, new PipeSlotProccess(name + "-PipeSlotProccess-" + i, _host.getBigMQ()));
            workThread.start();

        }

        //间隔一定时间监控运行状况
        scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            public void run() {
                s_logger.info("------");

                //取出的消息数量
                int _receiveFromMqDataCount = receiveFromMqDataCount.get();
                int newReceiveFromMqDataCount = _receiveFromMqDataCount - lastReceiveFromMqDataCount;
                lastReceiveFromMqDataCount = _receiveFromMqDataCount;

                //保存到bigtable的数据条数
                int _saveToBigtableDataCount = saveToBigtableDataCount.get();
                int newSaveToBigtableDataCount = _saveToBigtableDataCount - lastSaveToBigtableDataCount;
                lastSaveToBigtableFailedDataCount = _saveToBigtableDataCount;

                //保存到bigtable失败的数据条数
                int _saveToBigtableFailedDataCount = saveToBigtableFailedDataCount.get();
                int newSaveToBigtableFailedDataCount = _saveToBigtableFailedDataCount - lastSaveToBigtableFailedDataCount;
                lastSaveToBigtableFailedDataCount = _saveToBigtableFailedDataCount;

                s_logger.info(PipeSlot.this.name + " in last  " + period + " s, total "
                        + newReceiveFromMqDataCount + " msg receive  from mq,total "
                        + newSaveToBigtableDataCount + " saved to bigtable,total " + newSaveToBigtableFailedDataCount + " failed save to bigtable!!");

            }
        }, period, period, TimeUnit.SECONDS);


        s_logger.info(name + "  start success!!!");


    }

    /**
     * 停止
     */
    public void stop() {
        isRuning = false;//等待线程自己结束
        scheduledExecutorService.shutdownNow();
    }


    /**
     * slot主要工作线程
     */
    private class PipeSlotProccess implements Runnable {
        /**
         * 线程名称
         */
        private String name;
        private IBigMQ iBigMQ;

        public PipeSlotProccess(String name, IBigMQ iBigMQ) {
            this.name = name;
            this.iBigMQ = iBigMQ;
        }


        @Override
        public String toString() {
            return name;
        }

        @Override
        public void run() {

            while (isRuning) {
                List<MQMsg> msgList = iBigMQ.batchReceive(BATCH_RECEIVE_SIZE);


                if (null == msgList || 0 == msgList.size()) {
                    s_logger.debug(name + "  receive no  message !!");
                    try {
                        Thread.sleep(3000);
                    } catch (InterruptedException e) {
                    }

                    continue;
                } else {//监控数据
                    receiveFromMqDataCount.incrementAndGet();
                }


                for (MQMsg m : msgList) {
                    DataPack dp = null;

                    try {
                        dp = DataPack.deserializeFromBytes(m.getData());
                        s_logger.debug("DataPack:" + dp.toString());

                        IDataParser dataParser = getDataParser(m.getMark());
                        if (null == dataParser) {
                            s_logger.error(" not support "+m.getMark());
                            continue;
                        }

                        //第二步解析
                        List<DataPackTarget> dataPackTargetList = dataParser.extractBody(dp);

                        if (null == dataPackTargetList || 0 == dataPackTargetList.size()) {
                            s_logger.error("extractBody  null dataPackTargetList," + m + dp);
                            continue;
                        }


                        for (DataPackTarget target : dataPackTargetList) {
                            s_logger.debug(target.toString());
                            //保存
                            saveDataTarget(target);
                            //TODO 分发
                            dispatchDataPack(target);
                        }


                    } catch (Exception e) {
                        e.printStackTrace();
                        s_logger.error("deal with msg error " + m + "\n" + e.getMessage());
                    } finally {
                        if (null != dp) {
                            dp.freeBuf();
                        }
                    }

                }

            }

            //停止后释放连接
            iBigMQ.releaseCurrentConn();
        }
    }

    /**
     * 获取解析器对象
     *
     * @param protocol
     * @return
     */
    private IDataParser getDataParser(String protocol) {
        IDataParser dataParser = dataParserCache.get(protocol);
        if (null != dataParser) {
            return dataParser;
        }

        Class<?> clazz = DataParserManager.getDataParserClass(protocol);
        if (null == clazz) {
            s_logger.error("no such data paser : " + protocol);
            return null;
        }


        try {
            dataParser = (IDataParser) clazz.newInstance();

            if (null != dataParser) {
                dataParserCache.put(protocol, dataParser);
            }
        } catch (Exception e) {
            s_logger.error(clazz + " newInstance error!!! " + e.getMessage());
        }

        return dataParser;
    }


    /**
     * 保存数据
     *
     * @param target
     */
    protected void saveDataTarget(DataPackTarget target) {

        DataPackObject dataPackObject = target.getDataPackObject();

        //获取vin码
        String vin = dataPackObject.getVin();

        if (StringUtil.isBlank(vin)) {//无vin码数据直接丢弃
            return;


            /*  //TODO  不提供无vin码的支持
            String deviceId = dataPackObject.getDeviceId();
            if (StringUtil.isBlank(deviceId)) {//没有vin码时候,设备ID+协议代替vin码
                s_logger.error("invalid data : no  vin or deviceId,");
                saveToBigtableFailedDataCount.incrementAndGet();
                return;
            }

            vin = dataPackObject.getProtocolName() + "-" + deviceId;

            */
        }



        //处理数据采集时间
        DateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmmssSSS");
        Date minDate = null;//小于1977年的日期都认为是非法的日期（40年前哪里有联网的车载设备）
        try {
            minDate = dateFormat.parse("19770101000000000");
        } catch (ParseException e) {//这里不会抛异常
            e.printStackTrace();
        }

        Date detectionDate = dataPackObject.getDetectionDate();
        String time = null;
        if (null == detectionDate || minDate.compareTo(detectionDate) > 0) {//日期没取到或是非法日期
            detectionDate = new Date();
            time = dateFormat.format(detectionDate) + "N";//加"N"表示系统生成的日期
            dataPackObject.setDetectionDate(detectionDate);
        } else {
            time = dateFormat.format(detectionDate);
        }


        //数据类型
        String dataType = DataPackObjectUtils.getDataType(dataPackObject);

        String rowKey = RowKeyUtil.makeRowKey(vin, dataType, time);
        s_logger.debug("$$$$$$$$" + dataPackObject);

        try {
            _host.saveDataTarget(rowKey, dataPackObject, DataPackObjectUtils.getTableName(dataType));
            saveToBigtableDataCount.incrementAndGet();
            s_logger.debug("save success");
        } catch (Exception e) {
            e.printStackTrace();
            s_logger.error("save failed   " + e.getMessage());
            saveToBigtableFailedDataCount.incrementAndGet();
        }
    }


    /**
     * 分发数据包
     *
     * @param target
     */
    protected void dispatchDataPack(DataPackTarget target) {//TODO 待实现
        DataPackObject dataPackObject = target.getDataPackObject();
        String vin = dataPackObject.getVin();
        if (StringUtil.isBlank(vin)) {return;}//无vin码数据丢弃


    }

}
