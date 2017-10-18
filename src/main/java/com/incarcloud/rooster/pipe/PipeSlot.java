package com.incarcloud.rooster.pipe;

import com.incarcloud.rooster.datapack.*;
import com.incarcloud.rooster.mq.IBigMQ;
import com.incarcloud.rooster.mq.MQMsg;
import com.incarcloud.rooster.util.DataPackObjectUtils;
import com.incarcloud.rooster.util.RowKeyUtil;
import com.incarcloud.rooster.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
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
    private static Logger s_logger = LoggerFactory.getLogger(PipeSlot.class);

    /**
     * 一批次接受消息的数量
     */
    private static final int BATCH_RECEIVE_SIZE = 10;
    /**
     * 接收数据线程数
     */
    private static final int RECIVE_DATA_THREAD_COUNT = 2;
    
    
//    private static final BlockingDeque<E>
    
    
    
    
    
    


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
        for (int i = 0; i < RECIVE_DATA_THREAD_COUNT; i++) {
            Thread workThread = new Thread(workThreadGroup, 
            		new PipeSlotReceiveDateProccess(name + "-PipeSlotProccess-" + i, 
            				_host.getReceiveDataMQ()));
            workThread.start();

        }

        //间隔一定时间监控运行状况
        scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            public void run() {

                //取出的消息数量
                int _receiveFromMqDataCount = receiveFromMqDataCount.get();
                int newReceiveFromMqDataCount = _receiveFromMqDataCount - lastReceiveFromMqDataCount;
                lastReceiveFromMqDataCount = _receiveFromMqDataCount;

                //保存到bigtable的数据条数
                int _saveToBigtableDataCount = saveToBigtableDataCount.get();
                int newSaveToBigtableDataCount = _saveToBigtableDataCount - lastSaveToBigtableDataCount;
                lastSaveToBigtableDataCount = _saveToBigtableDataCount;

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
    private class PipeSlotReceiveDateProccess implements Runnable {
        /**
         * 线程名称
         */
        private String name;
        private IBigMQ receiveDataMQ;

        public PipeSlotReceiveDateProccess(String name, IBigMQ receiveDataMQ) {
            this.name = name;
            this.receiveDataMQ = receiveDataMQ;
        }


        @Override
        public String toString() {
            return name;
        }

        @Override
        public void run() {

            while (isRuning) {
                List<MQMsg> msgList = receiveDataMQ.batchReceive(BATCH_RECEIVE_SIZE);


                if (null == msgList || 0 == msgList.size()) {
                    s_logger.debug(name + "  receive no  message !!");
                    try {
                        Thread.sleep(3000);
                    } catch (InterruptedException e) {
                    }

                    continue;
                } else {//监控数据
                    receiveFromMqDataCount.getAndAdd(msgList.size());
                }


                for (MQMsg m : msgList) {
                    DataPack dp = null;

                    try {
                        dp = DataPack.deserializeFromBytes(m.getData());
                        s_logger.debug("DataPack:" + dp.toString());

                        IDataParser dataParser = DataParserManager.getDataParser(dp.getProtocol());
                        if (null == dataParser) {
                            s_logger.error(" not support " + dp.getProtocol());
                            continue;
                        }

                        //第二步解析
                        List<DataPackTarget> dataPackTargetList = dataParser.extractBody(dp);//同一个DataPack解出的数据列表


                        if (null == dataPackTargetList || 0 == dataPackTargetList.size()) {
                            s_logger.info("extractBody  null dataPackTargetList," + m + dp);
                            continue;
                        }

                        for (DataPackTarget t:
                                dataPackTargetList) {
                            System.out.println("%%%%%%%%%%%"+t.toString());
                        }

                        saveDataPacks(dataPackTargetList, dp.getReceiveTime());//保存数据
                        dispatchDataPacks(dp,dataPackTargetList);//TODO 分发


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
            receiveDataMQ.releaseCurrentConn();
        }
    }


    /**
     * 获取vin码，无vin码则返回 设备id
     *
     * @param dataPackTargetList 数据对象数据列表(同一个DataPack解出的)
     * @return vin码
     */
    private String checkAndGetVin(List<DataPackTarget> dataPackTargetList) {
        Iterator<DataPackTarget> iter = dataPackTargetList.iterator();

        String vin = null;
        //处理无vin的数据
        while (iter.hasNext()) {
            DataPackObject dataPackObject = iter.next().getDataPackObject();
            String deviceId = dataPackObject.getDeviceId();
            String vin0 = dataPackObject.getVin();
            if(StringUtil.isBlank(vin0)  && StringUtil.isBlank(deviceId)){
                s_logger.error("invalid data : no  vin or deviceId,");
                iter.remove();
                continue;
            }


            if (StringUtil.isBlank(vin0)) {
                s_logger.debug("no vin," + dataPackObject);
                vin = deviceId;//没有vin码时候,设备ID代替vin码
//                dataPackObject.setVin(vin);//不再重置vin码
            } else {
                vin = vin0;
                break;
            }

        }

        return vin;
    }


    /**
     * 1.设置数据接收时间
     * 2.对采集时间字段为空或无效的数据进行处理、
     * 3.生成rowkey，返回 rowkey  ->  DataPackObject
     *
     * @param dataPackTargetList 数据列表(同一个DataPack解出的)
     * @param reciveTime         数据接收时间（gather服务器接收时间，非设备采集时间）
     * @param vin                vin码
     * @return 待保存的数据
     */
    private Map<String, DataPackObject> treatDetectionAndGetDataPackObject(List<DataPackTarget> dataPackTargetList, Date reciveTime, String vin) {
        Map<String, DataPackObject> dataForSave = new HashMap<>();
        //处理检测日期
        /**
         * 对于无采集时间或采集时间为非法时间，做如下处理：
         * 1.该批数据（属于同一个包）含有位置数据  且  位置数据带有合法采集时间，用位置数据的采集时间来重置所有包的采集时间，
         * 2.该批数据（属于同一个包）不含位置数据  或  位置数据不带有合法采集时间，用接收时间重置所有包的采集时间
         */
        Date reciveTime0 = reciveTime;
        for (DataPackTarget target : dataPackTargetList) {//获取位置时间覆盖接收时间
            if (target.getDataPackObject() instanceof DataPackPosition) {
                DataPackPosition position = (DataPackPosition) target.getDataPackObject();
                if (DataPackObjectUtils.isLegalDetectionDate(position.getPositionTime())) {
                    reciveTime0 = position.getPositionTime();
                    break;
                }

            }
        }


        for (DataPackTarget target : dataPackTargetList) {
            //1、校正非法采集时间
            DataPackObject packObject = target.getDataPackObject();
            packObject.setReceiveTime(reciveTime);


            String timeStr = null;
            if (DataPackObjectUtils.checkAndResetIlllegalDetectionDate(packObject, reciveTime0)) {//采集时间被接收时间重置
                timeStr = DataPackObjectUtils.convertDetectionDateToString(packObject.getDetectionTime()) + "N";//N表示设备未上传数据采集时间，系统自动加上采集时间
            } else {
                timeStr = DataPackObjectUtils.convertDetectionDateToString(packObject.getDetectionTime());
            }

            //2、保存
            String dataType = DataPackObjectUtils.getDataType(packObject);//数据类型
            String rowKey = RowKeyUtil.makeRowKey(vin, dataType, timeStr);

            dataForSave.put(rowKey, packObject);

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
            s_logger.error("saveVin error,vin=" + vin + "  " + e.getMessage());
        }

    }


    /**
     * 保存数据
     *
     * @param rowKey         bigtable主键列值
     * @param dataPackObject 数据对象
     * @param recieveTime    数据接收时间（gather服务器接收时间，非设备采集时间），二级索引用接收时间时间生成便于同步数据
     */
    protected void saveDataPackObject(String rowKey, DataPackObject dataPackObject, Date recieveTime) {

        System.out.println("$$$$$$$$"+rowKey+"," + dataPackObject);

        try {
            _host.saveDataPackObject(rowKey, dataPackObject, recieveTime);
            saveToBigtableDataCount.incrementAndGet();
            s_logger.debug("save success:" + rowKey);
        } catch (Exception e) {
            e.printStackTrace();
            s_logger.error("save failed   " + rowKey + "\n" + e.getMessage());
            saveToBigtableFailedDataCount.incrementAndGet();
        }
    }


    /**
     * 保存数据
     *
     * @param dataPackTargetList 数据列表(同一个DataPack解出的)
     * @param recieveTime        数据接收时间（gather服务器接收时间，非设备采集时间）
     */
    private void saveDataPacks(List<DataPackTarget> dataPackTargetList, Date recieveTime) {

        //获取vin码
        String vin = checkAndGetVin(dataPackTargetList);
        //保存vin码
        saveVin(vin);


        //处理采集时间,生成rowkey
        Map<String, DataPackObject> dataForSave = treatDetectionAndGetDataPackObject(dataPackTargetList, recieveTime, vin);
        for (Map.Entry<String, DataPackObject> data : dataForSave.entrySet()) {
            saveDataPackObject(data.getKey(), data.getValue(), recieveTime);
        }

    }


    /**
     * 分发数据包
     *
     * @param dp 原始数据包
     * @param dataPackTargetList 数据列表(同一个DataPack解出的)
     */
    private void dispatchDataPacks(DataPack dp,List<DataPackTarget> dataPackTargetList) {//TODO 待实现
    	/*for (DataPackTarget target : dataPackTargetList) {
            s_logger.debug(target.toString());

            DataPackObject dataPackObject = target.getDataPackObject();
            String vin = dataPackObject.getVin();
            if (StringUtil.isBlank(vin)) {
                continue;
            }//无vin码数据丢弃
        }*/
    	
    	
    	if(null != _host.getGbPushMQ()){//分发到国标
    		IBigMQ gbPushMQ = _host.getGbPushMQ();
    		
    		try {
				gbPushMQ.post(new MQMsg(dp.getMark(), dp.serializeToBytes()));
			} catch (UnsupportedEncodingException e) {
				s_logger.debug("Unsupported Encoding   UTF-8");
			}
    	}
    	
    	
    	/*if(null != _host.getDbPushMQ()){//分发到地标
    		
    	}*/


    }

}
