package com.lzj.zkmaster;

import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkException;
import org.I0Itec.zkclient.exception.ZkInterruptedException;
import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.I0Itec.zkclient.exception.ZkNodeExistsException;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * 服务器：开启  关闭  抢占master节点  释
 * 放master节点(释放时需要检测自身释放为master,只有master才能释放)
 */
public class WorkServer {
    private volatile boolean running = false;

    //保存服务器信息
    private RunningData serverData = null;
    //保存Master服务器的信息
    private RunningData masterData = null;

    //与zookeeper通信
    private ZkClient zkClient = null;
    //监听事件
    private IZkDataListener zkDataListener = null;
    //节点路径
    private static final String MASTER_PATH = "/master";

    private ScheduledExecutorService delayService = Executors.newScheduledThreadPool(1);
    private int delayTime = 5;

    public WorkServer(RunningData rd) {
        //初始化服务器信息
        this.serverData = rd;
        //初始化监听事件
        this.zkDataListener = new IZkDataListener() {
            public void handleDataDeleted(String s) throws Exception {
                //1、未优化的方式：
                //master节点被删除，直接抢占master
                //takeMaster();

                //2、应对网络抖动优化策略：
                //由于网络原因导致了上次的master节点被zookeeper误认为宕机而删除，
                //对于这种情况，判断当前服务器是否为上次的master节点
                // 如果是，则优先抢占master
                // 如果不是，则延迟5s后再抢占
                if(masterData != null && masterData.getName().equals(serverData.getName())) {
                    takeMaster();
                } else {
                    delayService.schedule(new Runnable() {
                        public void run() {
                            takeMaster();
                        }
                    }, delayTime, TimeUnit.SECONDS); //延迟5s后抢占master
                }
            }

            public void handleDataChange(String s, Object o) throws Exception {
            }
        };
    }

    public void start() throws Exception {
        if(running) {
            throw new Exception("服务器已启动");
        }

        running = true;
        //注册监听事件
        zkClient.subscribeDataChanges(MASTER_PATH, zkDataListener);
        //开始抢占master
        takeMaster();
    }

    public void stop() throws Exception {
        if(!running) {
            throw new Exception("服务器已停止");
        }

        running = false;
        //取消事件监听
        zkClient.unsubscribeDataChanges(MASTER_PATH, zkDataListener);
        //释放master
        releaseMaster();

    }

    public void takeMaster() {
        if(!running) {
            return;
        }

        //开始在zk上创建临时节点
        //1、创建成功，则说明抢占到了master节点，将自己的信息赋值给masterData变量保存
        //2、创建失败，则说明已有master节点存在，则获取当前master节点的信息
            //2.1 获取信息为空，则说明当前master节点宕机，本服务器可继续抢占master
            //2.2 获取信息不为空，将当前master信息赋值给masterData变量保存
        try {
            zkClient.createEphemeral(MASTER_PATH, serverData);
            masterData = serverData; //标记自己为master
            System.out.println(serverData.getName() + "is master");

            //为了方便演示，让抢占到的master只存在5s
            delayService.schedule(new Runnable() {
                public void run() {
                    if(checkMaster()) {
                        releaseMaster();
                    }
                }
            }, delayTime, TimeUnit.SECONDS);


        } catch (ZkNodeExistsException e) {
            RunningData currentMasterData = zkClient.readData(MASTER_PATH);
            if(currentMasterData != null) {
                this.masterData = currentMasterData;
            } else {
                takeMaster();
            }
        } catch (Exception e) {
            //ignore
        }
    }

    public void releaseMaster() {
        //1、检测自身是否是master
        if(checkMaster()) {
            zkClient.delete(MASTER_PATH);
        }
    }

    private boolean checkMaster() {
        try {
            RunningData currentMasterData = zkClient.readData(MASTER_PATH);
            this.masterData = currentMasterData;
            if(masterData.getName().equals(this.serverData.getName())) {
                return true;
            }

            return false;
        } catch (ZkNoNodeException e) {
            return false;
        } catch (ZkInterruptedException e) {
            return checkMaster();
        } catch (ZkException e) {
            return false;
        }
    }

    public ZkClient getZkClient() {
        return zkClient;
    }

    public void setZkClient(ZkClient zkClient) {
        this.zkClient = zkClient;
    }
}
