package org.frameworkset.elasticsearch.imp.cdc;
/**
 * Copyright 2023 bboss
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import com.frameworkset.util.SimpleStringUtil;
import org.frameworkset.session.TestVO;
import org.frameworkset.soa.ObjectSerializable;
import org.frameworkset.spi.geoip.IpInfo;
import org.frameworkset.tran.CommonRecord;
import org.frameworkset.tran.DataRefactor;
import org.frameworkset.tran.DataStream;
import org.frameworkset.tran.ExportResultHandler;
import org.frameworkset.tran.config.ImportBuilder;
import org.frameworkset.tran.context.Context;
import org.frameworkset.tran.plugin.custom.output.CustomOutPut;
import org.frameworkset.tran.plugin.custom.output.CustomOutputConfig;
import org.frameworkset.tran.plugin.mongocdc.MongoCDCInputConfig;
import org.frameworkset.tran.schedule.TaskContext;
import org.frameworkset.tran.task.TaskCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * <p>Description: </p>
 * <p></p>
 *
 * @author biaoping.yin
 * @Date 2023/11/1
 */
public class MongodbCDCDBStatusDemo {
    private static Logger logger = LoggerFactory.getLogger(MongodbCDCDBStatusDemo.class);
    /**
     * 启动运行同步作业主方法
     * @param args
     */
    public static void main(String[] args){

        MongodbCDCDBStatusDemo dbdemo = new MongodbCDCDBStatusDemo();
        dbdemo.scheduleImportData();
    }

    /**
     * 同步作业实现和运行方法
     */
    public void scheduleImportData() {


        // 5.2.4 编写同步代码
        //定义Mongodb到dummy数据同步组件
        ImportBuilder importBuilder = new ImportBuilder();
//		importBuilder.setStatusDbname("statusds");
//		importBuilder.setStatusTableDML(DBConfig.mysql_createStatusTableSQL);
        // 5.2.4.1 设置mongodb参数
        MongoCDCInputConfig mongoDBInputConfig = new MongoCDCInputConfig();
        mongoDBInputConfig.setName("session");
        mongoDBInputConfig.setEnableIncrement(true);
        mongoDBInputConfig.setIncludePreImage(true).setUpdateLookup(true)
                .setDbIncludeList("sessiondb").setCollectionIncludeList("sessionmonitor_sessions,session_sessions")
                .setConnectString("mongodb://192.168.137.1:27017,192.168.137.1:27018,192.168.137.1:27019/?replicaSet=rs0")
                .setConnectTimeout(10000)
                .setMaxWaitTime(10000)
                .setSocketTimeout(1500).setSocketKeepAlive(true)
                .setConnectionsPerHost(10)
//                .setServerAddresses("127.0.0.1:27017")//多个地址用回车换行符或者逗号分割：127.0.0.1:27017\n127.0.0.1:27018
                // mechanism 取值范围：PLAIN GSSAPI MONGODB-CR MONGODB-X509，默认为MONGODB-CR
                //String database,String userName,String password,String mechanism
                //https://www.iteye.com/blog/yin-bp-2064662
//				.buildClientMongoCredential("sessiondb","bboss","bboss","MONGODB-CR")
                ;


        importBuilder.setInputConfig(mongoDBInputConfig);

        CustomOutputConfig customOupputConfig = new CustomOutputConfig();
        //自己处理数据
        customOupputConfig.setCustomOutPut(new CustomOutPut() {
            @Override
            public void handleData(TaskContext taskContext, List<CommonRecord> datas) {

                //You can do any thing here for datas
                for (CommonRecord record : datas) {
                    Map<String, Object> data = record.getDatas();
                    //自行处理data数据，写hbase，clickhouse，hdfs，以及其他数据库
                    logger.info(SimpleStringUtil.object2json(data));
                }
            }
        });
        importBuilder.setOutputConfig(customOupputConfig);

        // 5.2.4.5 并行任务配置（可选步骤，可以不需要做以下配置）
        importBuilder.setParallel(true);//设置为多线程并行批量导入,false串行
        importBuilder.setQueue(10);//设置批量导入线程池等待队列长度
        importBuilder.setThreadCount(50);//设置批量导入线程池工作线程数量
        importBuilder.setContinueOnError(false);//任务出现异常，是否继续执行作业：true（默认值）继续执行 false 中断作业执行

        // 5.2.4.6 数据加工处理（可选步骤，可以不需要做以下配置）
        // 全局记录配置：打tag，标识数据来源于jdk timer
        importBuilder.addFieldValue("fromTag", "jdk timer");
        // 数据记录级别的转换处理（可选步骤，可以不需要做以下配置）
        importBuilder.setDataRefactor(new DataRefactor() {
            public void refactor(Context context) throws Exception {
                logger.info("context.isDelete():"+context.isDelete());
                logger.info("context.isUpdate():"+context.isUpdate());
                logger.info("context.isInsert():"+context.isInsert());
                String id = context.getStringValue("_id");
                //根据字段值忽略对应的记录，这条记录将不会被同步到elasticsearch中
                if (id.equals("5dcaa59e9832797f100c6806"))
                    context.setDrop(true);
                //添加字段extfiled2到记录中，值为2
                context.addFieldValue("extfiled2", 2);
                //添加字段extfiled到记录中，值为1
                context.addFieldValue("extfiled", 1);
                boolean httpOnly = context.getBooleanValue("httpOnly");
                boolean secure = context.getBooleanValue("secure");
                String shardNo = context.getStringValue("shardNo");
                if (shardNo != null) {
                    //利用xml序列化组件将xml报文序列化为一个Integer
                    context.addFieldValue("shardNo", ObjectSerializable.toBean(shardNo, Integer.class));
                } else {
                    context.addFieldValue("shardNo", 0);
                }
                //空值处理
                String userAccount = context.getStringValue("userAccount");
                if (userAccount == null)
                    context.addFieldValue("userAccount", "");
                else {
                    //利用xml序列化组件将xml报文序列化为一个String
                    context.addFieldValue("userAccount", ObjectSerializable.toBean(userAccount, String.class));
                }
                //空值处理
                String testVO = context.getStringValue("testVO");
                if (testVO == null)
                    context.addFieldValue("testVO", "");
                else {
                    //利用xml序列化组件将xml报文序列化为一个TestVO
                    TestVO testVO1 = ObjectSerializable.toBean(testVO, TestVO.class);
                    context.addFieldValue("testVO", testVO1);
                }
                //空值处理
                String privateAttr = context.getStringValue("privateAttr");
                if (privateAttr == null) {
                    context.addFieldValue("privateAttr", "");
                } else {
                    //利用xml序列化组件将xml报文序列化为一个String
                    context.addFieldValue("privateAttr", ObjectSerializable.toBean(privateAttr, String.class));
                }
                //空值处理
                String local = context.getStringValue("local");
                if (local == null)
                    context.addFieldValue("local", "");
                else {
                    //利用xml序列化组件将xml报文序列化为一个String
                    context.addFieldValue("local", ObjectSerializable.toBean(local, String.class));
                }
                //将long类型的lastAccessedTime字段转换为日期类型
                long lastAccessedTime = context.getLongValue("lastAccessedTime");
                context.addFieldValue("lastAccessedTime", new Date(lastAccessedTime));
                //将long类型的creationTime字段转换为日期类型
                long creationTime = context.getLongValue("creationTime");
                context.addFieldValue("creationTime", new Date(creationTime));
                //根据session访问客户端ip，获取对应的客户地理位置经纬度信息、运营商信息、省地市信息IpInfo对象
                //并将IpInfo添加到Elasticsearch文档中
                String referip = context.getStringValue("referip");
                if (referip != null) {
                    IpInfo ipInfo = context.getIpInfoByIp(referip);
                    if (ipInfo != null)
                        context.addFieldValue("ipInfo", ipInfo);
                }
                //除了通过context接口获取mongodb的记录字段，还可以直接获取当前的mongodb记录，可自行利用里面的值进行相关处理
                Map record = (Map) context.getRecord();
            }
        });

        // 5.2.4.7 设置同步作业结果回调处理函数（可选步骤，可以不需要做以下配置）
        //设置任务处理结果回调接口
        importBuilder.setExportResultHandler(new ExportResultHandler<Object, String>() {
            @Override
            public void success(TaskCommand<Object, String> taskCommand, String result) {
                System.out.println(taskCommand.getTaskMetrics());//打印任务执行情况
            }

            @Override
            public void error(TaskCommand<Object, String> taskCommand, String result) {
                System.out.println(taskCommand.getTaskMetrics());//打印任务执行情况
                /**
                 //分析result，提取错误数据修改后重新执行,
                 Object datas = taskCommand.getDatas();
                 Object errorDatas = ... //分析result,从datas中提取错误数据，并设置到command中，通过execute重新执行任务
                 taskCommand.setDatas(errorDatas);
                 taskCommand.execute();
                 */
            }

            @Override
            public void exception(TaskCommand<Object, String> taskCommand, Throwable exception) {
                System.out.println(taskCommand.getTaskMetrics());//打印任务执行情况
            }


        });

        /**
         // 5.2.4.8 自定义Elasticsearch索引文档id生成机制（可选步骤，可以不需要做以下配置）
         //自定义Elasticsearch索引文档id生成机制
         importBuilder.setEsIdGenerator(new EsIdGenerator() {
         //如果指定EsIdGenerator，则根据下面的方法生成文档id，
         // 否则根据setEsIdField方法设置的字段值作为文档id，
         // 如果默认没有配置EsIdField和如果指定EsIdGenerator，则由es自动生成文档id

         @Override public Object genId(Context context) throws Exception {
         return SimpleStringUtil.getUUID();//返回null，则由es自动生成文档id
         }
         });*/

        // 5.2.4.9 设置增量字段信息（可选步骤，全量同步不需要做以下配置）
        //增量配置开始

//        importBuilder.setFromFirst(true);//任务重启时，重新开始采集数据，true 重新开始，false不重新开始，适合于每次全量导入数据的情况，如果是全量导入，可以先删除原来的索引数据
//        importBuilder.setLastValueStorePath("mongodbcdc_import");//记录上次采集的增量字段值的文件路径，作为下次增量（或者重启后）采集数据的起点，不同的任务这个路径要不一样
        importBuilder.setStatusDbname("test");

        // 5.2.4.10 执行作业
        /**
         * 构建DataStream，执行mongodb数据到dummy的同步操作
         */
        DataStream dataStream = importBuilder.builder();
        dataStream.execute();//执行同步操作
        logger.info("dataStream started.");
    }
}
