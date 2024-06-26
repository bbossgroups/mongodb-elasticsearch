package org.frameworkset.elasticsearch.imp;
/**
 * Copyright 2008 biaoping.yin
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

import com.mongodb.BasicDBObject;
import com.xxl.job.core.util.ShardingUtil;
import org.bson.Document;
import org.frameworkset.session.TestVO;
import org.frameworkset.soa.ObjectSerializable;
import org.frameworkset.spi.geoip.IpInfo;
import org.frameworkset.tran.DataRefactor;
import org.frameworkset.tran.ExportResultHandler;
import org.frameworkset.tran.config.ImportBuilder;
import org.frameworkset.tran.context.Context;
import org.frameworkset.tran.plugin.es.output.ElasticsearchOutputConfig;
import org.frameworkset.tran.plugin.mongodb.input.MongoDBInputConfig;
import org.frameworkset.tran.schedule.ExternalScheduler;
import org.frameworkset.tran.schedule.xxjob.AbstractXXLJobHandler;
import org.frameworkset.tran.task.TaskCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * <p>Description: 使用xxl-job,quartz等外部定时任务调度引擎导入数据，需要设置：</p>
 * importBuilder.setExternalTimer(true);
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2019/4/13 13:45
 * @author biaoping.yin
 * @version 1.0
 */
public class XXJobMongodb2ESImportTask extends AbstractXXLJobHandler {
	private static Logger logger = LoggerFactory.getLogger(XXJobMongodb2ESImportTask.class);
	public void init(){
		// 可参考Sample示例执行器中的示例任务"ShardingJobHandler"了解试用

		externalScheduler = new ExternalScheduler();
		externalScheduler.dataStream((Object params)->{

			logger.info("params:>>>>>>>>>>>>>>>>>>>" + params);
			// 5.2.4 编写同步代码
			//定义Mongodb到Elasticsearch数据同步组件
			ImportBuilder importBuilder = new ImportBuilder();
//		importBuilder.setStatusDbname("statusds");
//		importBuilder.setStatusTableDML(DBConfig.mysql_createStatusTableSQL);
			// 5.2.4.1 设置mongodb参数
			MongoDBInputConfig mongoDBInputConfig = new MongoDBInputConfig();
			mongoDBInputConfig.setName("session")
					.setDb("sessiondb")
					.setDbCollection("sessionmonitor_sessions")
					.setConnectTimeout(10000)
					.setWriteConcern("JOURNAL_SAFE")
					.setReadPreference("")
					.setMaxWaitTime(10000)
					.setSocketTimeout(1500).setSocketKeepAlive(true)
					.setConnectionsPerHost(100)
					.setServerAddresses("127.0.0.1:27017")//多个地址用回车换行符分割：127.0.0.1:27017\n127.0.0.1:27018
					// mechanism 取值范围：PLAIN GSSAPI MONGODB-CR MONGODB-X509，默认为MONGODB-CR
					//String database,String userName,String password,String mechanism
					//https://www.iteye.com/blog/yin-bp-2064662
//				.buildClientMongoCredential("sessiondb","bboss","bboss","MONGODB-CR")
//				.setOption("")
					;

			//定义mongodb数据查询条件对象（可选步骤，全量同步可以不需要做条件配置）
			BasicDBObject query = new BasicDBObject();

			// 提取集群节点分片号，将分片号作为检索同步数据的条件,实现分片同步功能
			ShardingUtil.ShardingVO shardingVO = ShardingUtil.getShardingVo();
			int index = 0;
			if(shardingVO != null) {
				index = shardingVO.getIndex();
				logger.info("index:>>>>>>>>>>>>>>>>>>>" + shardingVO.getIndex());
				logger.info("total:>>>>>>>>>>>>>>>>>>>" + shardingVO.getTotal());
			}
			try {
				String idxStr = ObjectSerializable.toXML(index);
				query.append("shardNo",idxStr );
			} catch (Exception e) {
				e.printStackTrace();
			}


			// 设定检索mongdodb session数据时间范围条件
			SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
			try {
				Date start_date = format.parse("1099-01-01");
				Date end_date = format.parse("2999-01-01");
				query.append("creationTime",
						new BasicDBObject("$gte", start_date.getTime()).append(
								"$lte", end_date.getTime()));
			}
			catch (Exception e){
				e.printStackTrace();
			}
			/**
			 // 设置按照host字段值进行正则匹配查找session数据条件（可选步骤，全量同步可以不需要做条件配置）
			 String host = "169.254.252.194-DESKTOP-U3V5C85";
			 Pattern hosts = Pattern.compile("^" + host + ".*$",
			 Pattern.CASE_INSENSITIVE);
			 query.append("host", new BasicDBObject("$regex",hosts));*/
			mongoDBInputConfig.setQuery(query);

			//设定需要返回的session数据字段信息（可选步骤，同步全部字段时可以不需要做下面配置）
			List<String> fetchFields = new ArrayList<>();
			fetchFields.add("appKey");
			fetchFields.add("sessionid");
			fetchFields.add("creationTime");
			fetchFields.add("lastAccessedTime");
			fetchFields.add("maxInactiveInterval");
			fetchFields.add("referip");
			fetchFields.add("_validate");
			fetchFields.add("host");
			fetchFields.add("requesturi");
			fetchFields.add("lastAccessedUrl");
			fetchFields.add("secure");
			fetchFields.add("httpOnly");
			fetchFields.add("lastAccessedHostIP");

			fetchFields.add("userAccount");
			fetchFields.add("testVO");
			fetchFields.add("privateAttr");
			fetchFields.add("local");
			fetchFields.add("shardNo");

			mongoDBInputConfig.setFetchFields(fetchFields);
			importBuilder.setInputConfig(mongoDBInputConfig);
			// 5.2.4.3 导入elasticsearch参数配置
			ElasticsearchOutputConfig elasticsearchOutputConfig = new ElasticsearchOutputConfig();
			elasticsearchOutputConfig
					.setEsIdField("_id")//设置文档主键，不设置，则自动产生文档id,直接将mongodb的ObjectId设置为Elasticsearch的文档_id
					.setIndex("mongodbdemo") ;//必填项，索引名称
//					.setIndexType("mongodbdemo") //es 7以后的版本不需要设置indexType或者设置为_doc，es7以前的版本必需设置indexType
//				.setRefreshOption("refresh")//可选项，null表示不实时刷新，importBuilder.setRefreshOption("refresh");表示实时刷新
			importBuilder.setOutputConfig(elasticsearchOutputConfig);
			importBuilder.setPrintTaskLog(true) //可选项，true 打印任务执行日志（耗时，处理记录数） false 不打印，默认值false
					.setBatchSize(10)  //可选项,批量导入es的记录数，默认为-1，逐条处理，> 0时批量处理
					.setFetchSize(100)  //按批从mongodb拉取数据的大小

					.setContinueOnError(true); // 忽略任务执行异常，任务执行过程抛出异常不中断任务执行

			// 5.2.4.5 并行任务配置（可选步骤，可以不需要做以下配置）
			importBuilder.setParallel(true);//设置为多线程并行批量导入,false串行
			importBuilder.setQueue(10);//设置批量导入线程池等待队列长度
			importBuilder.setThreadCount(50);//设置批量导入线程池工作线程数量
			importBuilder.setContinueOnError(true);//任务出现异常，是否继续执行作业：true（默认值）继续执行 false 中断作业执行

			// 5.2.4.6 数据加工处理（可选步骤，可以不需要做以下配置）
			// 全局记录配置：打tag，标识数据来源于xxljob
			 importBuilder.addFieldValue("fromTag","xxljob");
			// 数据记录级别的转换处理（可选步骤，可以不需要做以下配置）
			importBuilder.setDataRefactor(new DataRefactor() {
				public void refactor(Context context) throws Exception  {
					String id = context.getStringValue("_id");
					//根据字段值忽略对应的记录，这条记录将不会被同步到elasticsearch中
					if(id.equals("5dcaa59e9832797f100c6806"))
						context.setDrop(true);
					//添加字段extfiled2到记录中，值为2
					context.addFieldValue("extfiled2",2);
					//添加字段extfiled到记录中，值为1
					context.addFieldValue("extfiled",1);
					boolean httpOnly = context.getBooleanValue("httpOnly");
					boolean secure = context.getBooleanValue("secure");
					String shardNo = context.getStringValue("shardNo");
					if(shardNo != null){
						context.addFieldValue("shardNo", ObjectSerializable.toBean(shardNo,Integer.class));
					}
					else{
						context.addFieldValue("shardNo", 0);
					}
					//空值处理
					String userAccount = context.getStringValue("userAccount");
					if(userAccount == null)
						context.addFieldValue("userAccount","");
					else{
						context.addFieldValue("userAccount", ObjectSerializable.toBean(userAccount,String.class));
					}
					//空值处理
					String testVO = context.getStringValue("testVO");
					if(testVO == null)
						context.addFieldValue("testVO","");
					else{
						context.addFieldValue("testVO", ObjectSerializable.toBean(testVO, TestVO.class));
					}
					//空值处理
					String privateAttr = context.getStringValue("privateAttr");
					if(privateAttr == null) {
						context.addFieldValue("privateAttr", "");
					}
					else{
						context.addFieldValue("privateAttr", ObjectSerializable.toBean(privateAttr, String.class));
					}
					//空值处理
					String local = context.getStringValue("local");
					if(local == null)
						context.addFieldValue("local","");
					else{
						context.addFieldValue("local", ObjectSerializable.toBean(local, String.class));
					}
					//将long类型的lastAccessedTime字段转换为日期类型
					long lastAccessedTime = context.getLongValue("lastAccessedTime");
					context.addFieldValue("lastAccessedTime",new Date(lastAccessedTime));
					//将long类型的creationTime字段转换为日期类型
					long creationTime = context.getLongValue("creationTime");
					context.addFieldValue("creationTime",new Date(creationTime));
					//根据session访问客户端ip，获取对应的客户地理位置经纬度信息、运营商信息、省地市信息IpInfo对象
					//并将IpInfo添加到Elasticsearch文档中
					String referip = context.getStringValue("referip");
					if(referip != null){
						IpInfo ipInfo = context.getIpInfoByIp(referip);
						if(ipInfo != null)
							context.addFieldValue("ipInfo",ipInfo);
					}
					//除了通过context接口获取mongodb的记录字段，还可以直接获取当前的mongodb记录，可自行利用里面的值进行相关处理
					Document record = (Document) context.getRecord();
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
					System.out.println(result);
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
					logger.error("",exception);
				}


			});

			// 5.2.4.9 设置增量字段信息（可选步骤，全量同步不需要做以下配置）
			//增量配置开始
			importBuilder.setLastValueColumn("lastAccessedTime");//手动指定数字增量查询字段
			importBuilder.setFromFirst(false);//任务重启时，重新开始采集数据，true 重新开始，false不重新开始，适合于每次全量导入数据的情况，如果是全量导入，可以先删除原来的索引数据
			//设置增量查询的起始值lastvalue
			try {
				Date date = format.parse("2000-01-01");
				importBuilder.setLastValue(date.getTime());
			}
			catch (Exception e){
				e.printStackTrace();
			}
			// 直接返回importBuilder组件
			return importBuilder;
		});

	}


}
