package com.incarcloud.pipe;

import com.incarcloud.rooster.datapack.*;
import com.incarcloud.rooster.datatarget.DataTarget;
import com.incarcloud.rooster.mq.MQMsg;
import com.incarcloud.rooster.util.DataTargetUtils;
import com.incarcloud.rooster.util.MQMsgUtil;
import com.incarcloud.rooster.util.RowKeyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

/**
 * @author Xiong Guanghua
 * @Description: 管道槽, 一个管道槽对应一种协议
 * @date 2017年6月2日 下午3:55:17
 */
public class PipeSlot {
    private static Logger s_logger = LoggerFactory.getLogger(PipeSlot.class);
    /**
     * 一批次接受消息的数量
     */
    private static int BATCH_RECEIVE_SIZE = 16;

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


    public void start() {
        s_logger.info(name + " start  receive  message !!");
        new Thread(new PipeSlotProccess(name+"-PipeSlotProccess")).start();
    }

    public void stop() {

    }


    private class PipeSlotProccess implements Runnable {
        private String name;

        public  PipeSlotProccess(String name){
            this.name = name;
        }


        @Override
        public String toString() {
            return name;
        }

        @Override
        public void run() {
            while (true) {
                List<MQMsg> msgList = batchReceive(BATCH_RECEIVE_SIZE);


                if (null == msgList) {
                    s_logger.debug(name + "  receive no  message !!");
                    try {
                        Thread.sleep(3000);
                    } catch (InterruptedException e) {
                    }

                    continue;
                }


                for (MQMsg m : msgList) {

//                System.out.println(MQMsgUtil.convertMQMsgToStr(m));
                    DataPack dp = MQMsgUtil.convertMQMsgToDataPack(m);


                    Class<?> clazz = null;
                    try {
                        clazz = DataParserManager.getDataParserClass(m.getMark());
//                        System.out.println("^^^^^^^^^^^^^"+m.getMark()+" "+clazz);
                        if (null == clazz) {
                            s_logger.error("no such data paser : " + m.getMark());
                            continue;
                        }
                        IDataParser dataParser = (IDataParser) clazz.newInstance();

                        //第二步解析
                        List<DataPackTarget> dataPackTargetList = dataParser.extractBody(dp);
                        if(null == dataPackTargetList || 0 == dataPackTargetList.size()){
                            s_logger.error("extractBody  null dataPackTargetList,"+dp);

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
//                        s_logger.error("data paser constructor error : " + m.getMark() + "  " + clazz);
                    }

                }


            }
        }
    }


    /**
     * 批量接收消息
     *
     * @param size 消息数量
     * @return
     */
    protected List<MQMsg> batchReceive(int size) {
        return _host.batchReceive(size);
    }


    /**
     * 保存数据
     *
     * @param target
     */
    protected void saveDataTarget(DataPackTarget target) {
        DataTarget dataTarget = target.getDataTarget();
        ETargetType type = target.getTargetType();
        String time = new SimpleDateFormat("yyyyMMddHHmmssSSS").format(dataTarget.getDetectionDate());

        String rowKey = RowKeyUtil.makeRowKey(dataTarget.getVin(),type.toString(),time);

        try {
            _host.saveDataTarget(rowKey, dataTarget, DataTargetUtils.getTableName(type));
        }catch (Exception e){
            s_logger.error(e.getMessage());
        }

    }


    /**
     * 分发数据包
     *
     * @param target
     */
    protected void dispatchDataPack(DataPackTarget target) {//TODO 待实现

    }

}
