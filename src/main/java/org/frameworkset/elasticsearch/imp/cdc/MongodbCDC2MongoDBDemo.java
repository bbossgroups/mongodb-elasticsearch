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
import org.frameworkset.tran.cdc.TableMapping;
import org.frameworkset.tran.config.ImportBuilder;
import org.frameworkset.tran.context.Context;
import org.frameworkset.tran.plugin.custom.output.CustomOutPut;
import org.frameworkset.tran.plugin.custom.output.CustomOutputConfig;
import org.frameworkset.tran.plugin.mongocdc.MongoCDCInputConfig;
import org.frameworkset.tran.plugin.mongodb.output.MongoDBOutputConfig;
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
public class MongodbCDC2MongoDBDemo {
    private static Logger logger = LoggerFactory.getLogger(MongodbCDC2MongoDBDemo.class);
    /**
     * 启动运行同步作业主方法
     * @param args
     */
    public static void main(String[] args){

        MongodbCDC2MongoDBDemo dbdemo = new MongodbCDC2MongoDBDemo();
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
        MongoCDCInputConfig mongoCDCInputConfig = new MongoCDCInputConfig();
        mongoCDCInputConfig.setName("session");
        mongoCDCInputConfig.setEnableIncrement(true);
        mongoCDCInputConfig.setIncludePreImage(true)
		        .setUpdateLookup(true)
                .setDbIncludeList("sessiondb")
		        .setCollectionIncludeList("sessionmonitor_sessions,session_sessions")
                .setConnectString("mongodb://192.168.137.1:27017,192.168.137.1:27018,192.168.137.1:27019/?replicaSet=rs0")
                .setConnectTimeout(10000)
                .setMaxWaitTime(10000)
                .setSocketTimeout(1500).setSocketKeepAlive(true)
                .setConnectionsPerHost(100)
//                .setServerAddresses("127.0.0.1:27017")//多个地址用回车换行符或者逗号分割：127.0.0.1:27017\n127.0.0.1:27018
                // mechanism 取值范围：PLAIN GSSAPI MONGODB-CR MONGODB-X509，默认为MONGODB-CR
                //String database,String userName,String password,String mechanism
                //https://www.iteye.com/blog/yin-bp-2064662
//				.setUserName("bboss")
//		        .setPassword("bboss")
//		        .setMechanism("MONGODB-CR")
//		        .setAuthDb("sessiondb")
                ;


        importBuilder.setInputConfig(mongoCDCInputConfig);

	    MongoDBOutputConfig mongoDBOutputConfig = new MongoDBOutputConfig();
	    mongoDBOutputConfig.setMultiCollections(true);

	    mongoDBOutputConfig.setName("testes2mg")
			    .setDb("testdb1")
			    .setDbCollection("demo")
			    .setConnectTimeout(10000)
			    .setWriteConcern("JOURNAL_SAFE")

			    .setMaxWaitTime(10000)
			    .setSocketTimeout(1500).setSocketKeepAlive(true)
			    .setConnectionsPerHost(10)
			    .setConnectString("mongodb://192.168.137.1:27017,192.168.137.1:27018,192.168.137.1:27019/?replicaSet=rs0")//多个地址用回车换行符分割：127.0.0.1:27017\n127.0.0.1:27018
	    // mechanism 取值范围：PLAIN GSSAPI MONGODB-CR MONGODB-X509，默认为MONGODB-CR
	    //String database,String userName,String password,String mechanism
	    //https://www.iteye.com/blog/yin-bp-2064662
//				.setUserName("bboss")
//		        .setPassword("bboss")
//		        .setMechanism("MONGODB-CR");
	    ;

	    mongoDBOutputConfig.setObjectIdField("_id");//全局指定文档_id值对应的字段
	    importBuilder.setOutputConfig(mongoDBOutputConfig);

        // 5.2.4.5 并行任务配置（可选步骤，可以不需要做以下配置）
        importBuilder.setParallel(true);//设置为多线程并行批量导入,false串行
        importBuilder.setQueue(10);//设置批量导入线程池等待队列长度
        importBuilder.setThreadCount(50);//设置批量导入线程池工作线程数量
        importBuilder.setContinueOnError(false);//任务出现异常，是否继续执行作业：true（默认值）继续执行 false 中断作业执行

        // 5.2.4.6 数据加工处理（可选步骤，可以不需要做以下配置）
        // 数据记录级别的转换处理（可选步骤，可以不需要做以下配置）
        importBuilder.setDataRefactor(new DataRefactor() {
            public void refactor(Context context) throws Exception {
                logger.info("context.isDelete():"+context.isDelete());
                logger.info("context.isUpdate():"+context.isUpdate());
				if(context.isUpdate()){
					context.markRecoredReplace();//采用替代模式更新修改数据，验证功能用，实际情况根据需要进行修改或者替代处理
				}
                logger.info("context.isInsert():"+context.isInsert());
				context.setRecordKeyField("_id");//记录级别指定文档_id值对应的字段
	            String table = (String)context.getMetaValue("table");//记录来源collection，默认输出表
	            String database = (String)context.getMetaValue("database");//记录来源db
	            TableMapping tableMapping = new TableMapping();
	            tableMapping.setTargetDatabase("testdb");//目标库
	            tableMapping.setTargetCollection("testcdc");//目标表
	            tableMapping.setTargetDatasource("testes2mg");//指定MongoDB数据源名称，对应一个MongoDB集群

	            context.setTableMapping(tableMapping);
	            logger.info("table:"+table);
	            logger.info("database:"+database);
            }
        });

        // 5.2.4.7 设置同步作业结果回调处理函数（可选步骤，可以不需要做以下配置）
        //设置任务处理结果回调接口
        importBuilder.setExportResultHandler(new ExportResultHandler<Object>() {
            @Override
            public void success(TaskCommand<Object> taskCommand, Object result) {
                System.out.println(taskCommand.getTaskMetrics());//打印任务执行情况
            }

            @Override
            public void error(TaskCommand<Object> taskCommand, Object result) {
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
            public void exception(TaskCommand<Object> taskCommand, Throwable exception) {
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
        importBuilder.setLastValueStorePath("mongodb2dbcdc_import");//记录上次采集的增量字段值的文件路径，作为下次增量（或者重启后）采集数据的起点，不同的任务这个路径要不一样


        // 5.2.4.10 执行作业
        /**
         * 构建DataStream，执行mongodb数据到dummy的同步操作
         */
        DataStream dataStream = importBuilder.builder();
        dataStream.execute();//执行同步操作
        logger.info("dataStream started.");
    }
}
