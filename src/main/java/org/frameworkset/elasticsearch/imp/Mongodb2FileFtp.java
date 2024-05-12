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
import org.bson.Document;
import org.frameworkset.elasticsearch.serial.SerialUtil;
import org.frameworkset.runtime.CommonLauncher;
import org.frameworkset.session.TestVO;
import org.frameworkset.soa.ObjectSerializable;
import org.frameworkset.spi.geoip.IpInfo;
import org.frameworkset.tran.CommonRecord;
import org.frameworkset.tran.DataRefactor;
import org.frameworkset.tran.DataStream;
import org.frameworkset.tran.ExportResultHandler;
import org.frameworkset.tran.config.ImportBuilder;
import org.frameworkset.tran.context.Context;
import org.frameworkset.tran.output.fileftp.FilenameGenerator;
import org.frameworkset.tran.output.ftp.FtpOutConfig;
import org.frameworkset.tran.plugin.file.output.FileOutputConfig;
import org.frameworkset.tran.plugin.mongodb.input.MongoDBInputConfig;
import org.frameworkset.tran.schedule.CallInterceptor;
import org.frameworkset.tran.schedule.TaskContext;
import org.frameworkset.tran.task.TaskCommand;
import org.frameworkset.tran.util.RecordGenerator;

import java.io.Writer;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2019/11/25 22:36
 * @author biaoping.yin
 * @version 1.0
 */
public class Mongodb2FileFtp {
	/**
	 * 启动运行同步作业主方法
	 * @param args
	 */
	public static void main(String[] args){

		Mongodb2FileFtp dbdemo = new Mongodb2FileFtp();
		dbdemo.scheduleImportData();
	}

	/**
	 * 同步作业实现和运行方法
	 */
	public void scheduleImportData(){


		// 5.2.4 编写同步代码
		//定义Mongodb到dummy数据同步组件
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
		// 5.2.4.3 导入dummy参数配置
		String ftpIp = CommonLauncher.getProperty("ftpIP","127.0.0.1");//同时指定了默认值
		FileOutputConfig fileFtpOupputConfig = new FileOutputConfig();
		FtpOutConfig ftpOutConfig = new FtpOutConfig();
		ftpOutConfig.setFtpIP(ftpIp);

		ftpOutConfig.setFtpPort(5322);
//		ftpOutConfig.addHostKeyVerifier("2a:da:5a:6a:cf:7d:65:e5:ac:ff:d3:73:7f:2c:55:c9");
		ftpOutConfig.setFtpUser("root");
		ftpOutConfig.setFtpPassword("123456");
		ftpOutConfig.setRemoteFileDir("/home/ecs/failLog");
		ftpOutConfig.setKeepAliveTimeout(100000);
		ftpOutConfig.setTransferEmptyFiles(false);
		ftpOutConfig.setFailedFileResendInterval(-1);
		ftpOutConfig.setBackupSuccessFiles(true);

		ftpOutConfig.setSuccessFilesCleanInterval(5000);
		ftpOutConfig.setFileLiveTime(86400);//设置上传成功文件备份保留时间，默认2天
		fileFtpOupputConfig.setFtpOutConfig(ftpOutConfig);
		fileFtpOupputConfig.setFileDir("D:\\workdir\\mondodb");
		fileFtpOupputConfig.setMaxFileRecordSize(20);//每20条记录生成一个文件
		//自定义文件名称
		fileFtpOupputConfig.setFilenameGenerator(new FilenameGenerator() {
			@Override
			public String genName(TaskContext taskContext, int fileSeq) {
				//fileSeq为切割文件时的文件递增序号
				String time = (String)taskContext.getTaskData("time");//从任务上下文中获取本次任务执行前设置时间戳
				String _fileSeq = fileSeq+"";
				int t = 6 - _fileSeq.length();
				if(t > 0){
					String tmp = "";
					for(int i = 0; i < t; i ++){
						tmp += "0";
					}
					_fileSeq = tmp+_fileSeq;
				}



				return "mongodb" + "_"+time +"_" + _fileSeq+".txt";
			}
		});
		//指定文件中每条记录格式，不指定默认为json格式输出
		fileFtpOupputConfig.setRecordGenerator(new RecordGenerator() {
			@Override
			public void buildRecord(TaskContext taskContext, CommonRecord record, Writer builder) {
				//直接将记录按照json格式输出到文本文件中
				SerialUtil.normalObject2json(record.getDatas(),//获取记录中的字段数据
						builder);
//				String data = (String)taskContext.getTaskData("data");//从任务上下文中获取本次任务执行前设置时间戳
//          System.out.println(data);

			}
		});
		importBuilder.setOutputConfig(fileFtpOupputConfig);
		importBuilder.setIncreamentEndOffset(300);//单位秒，同步从上次同步截止时间当前时间前5分钟的数据，下次继续从上次截止时间开始同步数据
//设置任务执行拦截器，可以添加多个
		importBuilder.addCallInterceptor(new CallInterceptor() {
			@Override
			public void preCall(TaskContext taskContext) {
				String formate = "yyyyMMddHHmmss";
				//HN_BOSS_TRADE00001_YYYYMMDDHHMM_000001.txt
				SimpleDateFormat dateFormat = new SimpleDateFormat(formate);
				String time = dateFormat.format(new Date());
				taskContext.addTaskData("time",time);
			}

			@Override
			public void afterCall(TaskContext taskContext) {
				System.out.println("afterCall 1");
			}

			@Override
			public void throwException(TaskContext taskContext, Throwable e) {
				System.out.println("throwException 1");
			}
		});
		// 5.2.4.4 jdk timer定时任务时间配置（可选步骤，可以不需要做以下配置）
		importBuilder.setFixedRate(false)//参考jdk timer task文档对fixedRate的说明
//					 .setScheduleDate(date) //指定任务开始执行时间：日期
				.setDeyLay(1000L) // 任务延迟执行deylay毫秒后执行
				.setPeriod(10000L); //每隔period毫秒执行，如果不设置，只执行一次

		// 5.2.4.5 并行任务配置（可选步骤，可以不需要做以下配置）
		importBuilder.setParallel(true);//设置为多线程并行批量导入,false串行
		importBuilder.setQueue(10);//设置批量导入线程池等待队列长度
		importBuilder.setThreadCount(50);//设置批量导入线程池工作线程数量
		importBuilder.setContinueOnError(true);//任务出现异常，是否继续执行作业：true（默认值）继续执行 false 中断作业执行

		// 5.2.4.6 数据加工处理（可选步骤，可以不需要做以下配置）
		// 全局记录配置：打tag，标识数据来源于jdk timer
		importBuilder.addFieldValue("fromTag","jdk timer");
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
					//利用xml序列化组件将xml报文序列化为一个Integer
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
					//利用xml序列化组件将xml报文序列化为一个String
					context.addFieldValue("userAccount", ObjectSerializable.toBean(userAccount,String.class));
				}
				//空值处理
				String testVO = context.getStringValue("testVO");
				if(testVO == null)
					context.addFieldValue("testVO","");
				else{
					//利用xml序列化组件将xml报文序列化为一个TestVO
					TestVO testVO1 = ObjectSerializable.toBean(testVO, TestVO.class);
					context.addFieldValue("testVO", testVO1);
				}
				//空值处理
				String privateAttr = context.getStringValue("privateAttr");
				if(privateAttr == null) {
					context.addFieldValue("privateAttr", "");
				}
				else{
					//利用xml序列化组件将xml报文序列化为一个String
					context.addFieldValue("privateAttr", ObjectSerializable.toBean(privateAttr, String.class));
				}
				//空值处理
				String local = context.getStringValue("local");
				if(local == null)
					context.addFieldValue("local","");
				else{
					//利用xml序列化组件将xml报文序列化为一个String
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

			@Override
			public Object genId(Context context) throws Exception {
				return SimpleStringUtil.getUUID();//返回null，则由es自动生成文档id
			}
		});*/

		// 5.2.4.9 设置增量字段信息（可选步骤，全量同步不需要做以下配置）
		//增量配置开始
		importBuilder.setLastValueColumn("lastAccessedTime");//手动指定数字增量查询字段
		importBuilder.setFromFirst(true);//任务重启时，重新开始采集数据，true 重新开始，false不重新开始，适合于每次全量导入数据的情况，如果是全量导入，可以先删除原来的索引数据
		importBuilder.setLastValueStorePath("mongodb_import");//记录上次采集的增量字段值的文件路径，作为下次增量（或者重启后）采集数据的起点，不同的任务这个路径要不一样
		//设置增量查询的起始值lastvalue
		try {
			Date date = format.parse("2000-01-01");
			importBuilder.setLastValue(date.getTime());
		}
		catch (Exception e){
			e.printStackTrace();
		}

		// 5.2.4.10 执行作业
		/**
		 * 构建DataStream，执行mongodb数据到dummy的同步操作
		 */
		DataStream dataStream = importBuilder.builder();
		dataStream.execute();//执行同步操作
	}
}
