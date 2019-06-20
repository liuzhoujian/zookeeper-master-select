package com.lzj.zkmaster;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.serialize.SerializableSerializer;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

/**
 * 启动类
 */
public class LeaderSelectorZkClient {
    //启动的服务个数
    private static final int        CLIENT_QTY = 10;
    //zookeeper服务器的地址
    private static final String     ZOOKEEPER_SERVER = "hadoop102:2181,hadoop103:2181,hadoop104:2181";

    public static void main(String[] args) {
        List<ZkClient> clients = new ArrayList<ZkClient>();
        List<WorkServer> servers = new ArrayList<WorkServer>();

        try {
            for(int i = 0; i < CLIENT_QTY; i++) {
                //创建zkClient
                ZkClient zkClient = new ZkClient(ZOOKEEPER_SERVER, 5000, 5000, new SerializableSerializer());
                clients.add(zkClient);

                //创建runningData
                RunningData runningData = new RunningData();
                runningData.setCid(Long.valueOf(i));
                runningData.setName("Client #" + i);

                //创建workServer
                WorkServer workServer = new WorkServer(runningData);
                workServer.setZkClient(zkClient);
                servers.add(workServer);
                workServer.start();
            }

            System.out.println("敲回车键退出！\n");
            new BufferedReader(new InputStreamReader(System.in)).readLine();

        } catch (Exception e) {
            e.printStackTrace();
        }  finally {
            System.out.println("Shutting down...");
            for (WorkServer workServer : servers) {
                try {
                    workServer.stop();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            for (ZkClient client : clients ) {
                try {
                    client.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
