package com.incarcloud.pipe;

import com.incarcloud.rooster.bigtable.IBigTable;
import com.incarcloud.rooster.datapack.DataPackObject;
import com.incarcloud.rooster.datapack.DataParserManager;
import com.incarcloud.rooster.mq.IBigMQ;
import com.incarcloud.rooster.mq.MQMsg;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

/**
 * @author Xiong Guanghua
 * @Description: 管道主机
 * @date 2017年6月2日 下午3:55:17
 */
public class PipeHost {
    private static Logger s_logger = LoggerFactory.getLogger(PipeHost.class);

    static {
        //加载com.incarcloud.rooster.datapack包下的所有类，使得解析器注册到DataParserManager
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
     * 操作消息队列接口
     */
    private IBigMQ bigMQ;

    /**
     * bigtable的操作接口
     */
    private IBigTable bigTable;


    /**
     * 是否已启动
     */
    private Boolean _bRunning = false;

    public PipeHost() {
        this("pipehost" + Calendar.getInstance().getTimeInMillis());
    }


    /**
     * @param name 主机名
     */
    public PipeHost(String name) {
        this.name = name;
//        _bossGroup = new NioEventLoopGroup();
//        _workerGroup = new NioEventLoopGroup();
    }


    /**
     * 启动
     */
    public void start() {
        if (_bRunning)
            return;

        if (0 == _slots.size()) {
            s_logger.error("no slot!!!");

            System.exit(-1);
        }

        for (PipeSlot slot : _slots) {
            slot.start();
        }

        _bRunning = true;
        s_logger.info(name + "start success!!");

    }

    /**
     * 停止
     */
    public void stop() {
        for (PipeSlot slot : _slots) {
            slot.stop();
        }

//        bigTable.close();
//        bigMQ.close();

        _bRunning = false;
    }


    /**
     * 批量接收消息
     *
     * @param size 消息数量
     * @return
     */
    public List<MQMsg> batchReceive(int size) {
        return bigMQ.batchReceive(size);
    }

    /**
     * 保存数据
     *
     * @param rowKey      行健
     * @param data        数据
     * @param recieveTime 二级索引用接收时间时间生成便于同步数据
     * @throws Exception
     */
    public void saveDataPackObject(String rowKey, DataPackObject data, Date recieveTime) throws Exception {
        bigTable.saveDataPackObject(rowKey, data,recieveTime);
    }

    /**
     * 保存vin码
     *
     * @param vin
     */
    public void saveVin(String vin) throws Exception {
        bigTable.saveVin(vin);
    }


    public void addSlot(PipeSlot slot) {
        _slots.add(slot);
    }

    public void setBigMQ(IBigMQ bigMQ) {
        this.bigMQ = bigMQ;
    }

    public IBigMQ getBigMQ() {
        return bigMQ;
    }


    public void setBigTable(IBigTable bigTable) {
        this.bigTable = bigTable;
    }

    public String getName() {
        return name;
    }
}
