package com.incarcloud.rooster.pipe;

import com.incarcloud.rooster.bigtable.IBigTable;
import com.incarcloud.rooster.datapack.DataPackObject;
import com.incarcloud.rooster.datapack.DataParserManager;
import com.incarcloud.rooster.mq.IBigMQ;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

/**
 * @author Xiong Guanghua
 * @Description: 管道主机
 * @date 2017年6月2日 下午3:55:17
 */
public class PipeHost {

    /**
     * Logger
     */
    private static Logger s_logger = LoggerFactory.getLogger(PipeHost.class);

    static {
        // 加载com.incarcloud.rooster.datapack包下的所有类，使得解析器注册到DataParserManager
        DataParserManager.loadClassOfSamePackage();
    }

    /**
     * 主机名
     */
    private String name;

    /**
     * 采集槽列表
     */
    private ArrayList<PipeSlot> _slots = new ArrayList<>();

    /**
     * 接收数据的mq
     */
    private IBigMQ receiveDataMQ;

    /**
     * 推送数据到国标的mq
     */
    private IBigMQ gbPushMQ;

    /**
     * 推送数据到地标的mq
     */
    private IBigMQ dbPushMQ;

    /**
     * 接收数据的Topic
     */
    private String receiveDataTopic;

    /**
     * 推送数据到国标的Topic
     */
    private String gbPushTopic;

    /**
     * 推送数据到地标的Topic
     */
    private String dbPushTopic;

    /**
     * bigtable的操作接口
     */
    private IBigTable bigTable;

    /**
     * 是否已启动
     */
    private Boolean _bRunning = false;

    /**
     * 默认构造函数
     */
    public PipeHost() {
        this("pipehost" + Calendar.getInstance().getTimeInMillis());
    }

    /**
     * 构造函数
     *
     * @param name 主机名
     */
    public PipeHost(String name) {
        this.name = name;
    }

    /**
     * 启动
     */
    public void start() {
        if (_bRunning) return;

        if (0 == _slots.size()) {
            s_logger.error("No slot!!!");
            System.exit(-1);
        }

        for (PipeSlot slot : _slots) {
            slot.start();
        }

        _bRunning = true;
        s_logger.info("{} start success!!!", name);
    }

    /**
     * 停止
     */
    public void stop() {
        for (PipeSlot slot : _slots) {
            slot.stop();
        }

        bigTable.close();
        receiveDataMQ.close();

        _bRunning = false;
    }

    /**
     * 批量接收消息
     *
     * @param size 消息数量
     * @return
     */
    public List<byte[]> batchReceive(int size) {
        return receiveDataMQ.batchReceive(receiveDataTopic, size);
    }

    /**
     * 保存数据
     *
     * @param rowKey 行健
     * @param data   车辆数据
     * @throws Exception
     */
    public void saveDataPackObject(String rowKey, DataPackObject data) throws Exception {
        bigTable.saveDataPackObject(rowKey, data);
    }

    /**
     * 保存vin码
     *
     * @param vin
     */
    public void saveVin(String vin) throws Exception {
        bigTable.saveVin(vin);
    }

    /**
     * 添加slot
     *
     * @param slot slot对象
     */
    public void addSlot(PipeSlot slot) {
        _slots.add(slot);
    }

    /**
     * 设置接收数据MQ
     *
     * @param bigMQ 接收数据MQ
     */
    public void setReceiveDataMQ(IBigMQ bigMQ) {
        this.receiveDataMQ = bigMQ;
    }

    /**
     * 获得接收数据MQ
     *
     * @return
     */
    public IBigMQ getReceiveDataMQ() {
        return receiveDataMQ;
    }

    /**
     * 设置bigTable对象
     *
     * @param bigTable bigTable对象
     */
    public void setBigTable(IBigTable bigTable) {
        this.bigTable = bigTable;
    }

    /**
     * 获得主机名
     *
     * @return
     */
    public String getName() {
        return name;
    }

    /**
     * 获得国标MQ
     *
     * @return
     */
    public IBigMQ getGbPushMQ() {
        return gbPushMQ;
    }

    /**
     * 设置国标MQ
     *
     * @param gbPushMQ 国标MQ
     */
    public void setGbPushMQ(IBigMQ gbPushMQ) {
        this.gbPushMQ = gbPushMQ;
    }

    /**
     * 获得地标MQ
     *
     * @return
     */
    public IBigMQ getDbPushMQ() {
        return dbPushMQ;
    }

    /**
     * 设置地标MQ
     *
     * @param dbPushMQ 地标MQ
     */
    public void setDbPushMQ(IBigMQ dbPushMQ) {
        this.dbPushMQ = dbPushMQ;
    }

    public String getReceiveDataTopic() {
        return receiveDataTopic;
    }

    public void setReceiveDataTopic(String receiveDataTopic) {
        this.receiveDataTopic = receiveDataTopic;
    }

    public String getGbPushTopic() {
        return gbPushTopic;
    }

    public void setGbPushTopic(String gbPushTopic) {
        this.gbPushTopic = gbPushTopic;
    }

    public String getDbPushTopic() {
        return dbPushTopic;
    }

    public void setDbPushTopic(String dbPushTopic) {
        this.dbPushTopic = dbPushTopic;
    }
}
