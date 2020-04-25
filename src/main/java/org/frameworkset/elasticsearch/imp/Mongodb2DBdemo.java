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

import com.frameworkset.util.SimpleStringUtil;
import com.mongodb.BasicDBObject;
import org.frameworkset.tran.DataRefactor;
import org.frameworkset.tran.DataStream;
import org.frameworkset.tran.ExportResultHandler;
import org.frameworkset.tran.context.Context;
import org.frameworkset.tran.mongodb.input.db.MongoDB2DBExportBuilder;
import org.frameworkset.tran.task.TaskCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Pattern;

/**
 * <p>Description: MongoDB-DB同步处理程序，如需调试同步功能直接运行main方法即可，
 * </p>
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2018/9/27 20:38
 * @author biaoping.yin
 * @version 1.0
 */
public class Mongodb2DBdemo {
	private static Logger logger = LoggerFactory.getLogger(Mongodb2DBdemo.class);
	public static void main(String args[]){
		Mongodb2DBdemo dbdemo = new Mongodb2DBdemo();

		dbdemo.scheduleTimestampImportData();
	}



	/**
	 * elasticsearch地址和数据库地址都从外部配置文件application.properties中获取，加载数据源配置和es配置
	 */
	public void scheduleTimestampImportData( ){
		MongoDB2DBExportBuilder importBuilder = MongoDB2DBExportBuilder.newInstance();
		//导入数据库和sql配置，数据库相关的配置已经在application.properties文件中配置，这里只需要指定sql配置文件和sqlName即可
		importBuilder.setSqlName("insertSQLnew") //指定将mongodb文档数据同步到数据库的sql语句名称，配置在sqlFile.xml中
					 .setSqlFilepath("sqlFile.xml");
		//mongodb的相关配置参数

		importBuilder.setName("session")
				.setDb("sessiondb")
				.setDbCollection("sessionmonitor_sessions")
				.setConnectTimeout(10000)
				.setWriteConcern("JOURNAL_SAFE")
				.setReadPreference("")
				.setMaxWaitTime(10000)
				.setSocketTimeout(1500).setSocketKeepAlive(true)
				.setConnectionsPerHost(100)
				.setThreadsAllowedToBlockForConnectionMultiplier(6)
				.setServerAddresses("127.0.0.1:27017")//多个地址用回车换行符分割：127.0.0.1:27017\n127.0.0.1:27018
				// mechanism 取值范围：PLAIN GSSAPI MONGODB-CR MONGODB-X509，默认为MONGODB-CR
				//String database,String userName,String password,String mechanism
				//https://www.iteye.com/blog/yin-bp-2064662
//				.buildClientMongoCredential("sessiondb","bboss","bboss","MONGODB-CR")
//				.setOption("")
				.setAutoConnectRetry(true);

		//设置检索mongodb数据条件
		BasicDBObject query = new BasicDBObject();
 		// 设定数据检索时间范围
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
		try {
			Date start_date = format.parse("1099-01-01");
			Date end_date = format.parse("2999-01-01");
			query.append("lastAccessedTime",
					new BasicDBObject("$gte", start_date.getTime()).append(
							"$lte", end_date.getTime()));
		}
		catch (Exception e){
			e.printStackTrace();
		}

		// 设置按照host字段值进行正则匹配查找数据
		String host = "169.254.252.194-DESKTOP-U3V5C85";
		Pattern hosts = Pattern.compile("^" + host + ".*$",
				Pattern.CASE_INSENSITIVE);
		query.append("host", new BasicDBObject("$regex",hosts));

		/** 添加其他过滤条件
		String referip = (String) queryParams.get("referip");
		if (!StringUtil.isEmpty(referip)) {
			Pattern referips = Pattern.compile("^" + referip + ".*$",
					Pattern.CASE_INSENSITIVE);
			query.append("referip", new BasicDBObject("$regex",referips));
		}

		String validate = (String) queryParams.get("validate");
		if (!StringUtil.isEmpty(validate)) {
			boolean _validate = Boolean.parseBoolean(validate);


			if(_validate)
			{
				query.append("_validate", _validate);
				BasicDBList values = new BasicDBList();
				values.add(new BasicDBObject("maxInactiveInterval", new BasicDBObject("$lte", 0)));
				values.add(new BasicDBObject("$where","return this.lastAccessedTime + this.maxInactiveInterval >="+ System.currentTimeMillis()));
				query.append("$or", values);
//				query.append("maxInactiveInterval", new BasicDBObject("$lte", 0));
//				query.append("lastAccessedTime + maxInactiveInterval", new BasicDBObject("$gte", System.currentTimeMillis()));
			}
			else
			{
				query.append("maxInactiveInterval", new BasicDBObject("$gt", 0));
				BasicDBList values = new BasicDBList();
				values.add(new BasicDBObject("_validate", false));

				values.add(new BasicDBObject("$where","return this.lastAccessedTime + this.maxInactiveInterval <"+ System.currentTimeMillis()));
				query.append("$or", values);
			}

		}
		 */
		importBuilder.setQuery(query);

		//设定需要召回的字段信息
		BasicDBObject fetchFields = new BasicDBObject();
		fetchFields.put("appKey", 1);
		fetchFields.put("sessionid", 1);
		fetchFields.put("creationTime", 1);
		fetchFields.put("lastAccessedTime", 1);
		fetchFields.put("maxInactiveInterval", 1);
		fetchFields.put("referip", 1);
		fetchFields.put("_validate", 1);
		fetchFields.put("host", 1);
		fetchFields.put("requesturi", 1);
		fetchFields.put("lastAccessedUrl", 1);
		fetchFields.put("secure",1);
		fetchFields.put("httpOnly", 1);
		fetchFields.put("lastAccessedHostIP", 1);

		fetchFields.put("userAccount",1);
		fetchFields.put("testVO", 1);
		fetchFields.put("privateAttr", 1);
		fetchFields.put("local", 1);
		importBuilder.setFetchFields(fetchFields);

		// 设置MongoDB检索选项
//		DBCollectionFindOptions dbCollectionFindOptions = new DBCollectionFindOptions();
//		dbCollectionFindOptions.maxTime(10000l, TimeUnit.MILLISECONDS);
//		importBuilder.setDbCollectionFindOptions(dbCollectionFindOptions);

		//全局忽略字段
//		importBuilder.addIgnoreFieldMapping("remark1");
		/**
		 * 获取数据
		 */
		importBuilder
				.setPrintTaskLog(true) //可选项，true 打印任务执行日志（耗时，处理记录数） false 不打印，默认值false
				.setBatchSize(3)  //可选项,批量导入DB的记录数，默认为-1，逐条处理，> 0时批量处理
				.setFetchSize(2); //按批从mongodb拉取数据的大小
		//定时任务配置，
		importBuilder.setFixedRate(false)//参考jdk timer task文档对fixedRate的说明
//					 .setScheduleDate(date) //指定任务开始执行时间：日期
				.setDeyLay(1000L) // 任务延迟执行deylay毫秒后执行
				.setPeriod(5000L); //每隔period毫秒执行，如果不设置，只执行一次
		//定时任务配置结束

		//增量配置开始
		importBuilder.setLastValueColumn("creationTime");//手动指定数字增量查询字段
		importBuilder.setFromFirst(true);//任务重启时，重新开始采集数据，true 重新开始，false不重新开始，适合于每次全量导入数据的情况，如果是全量导入，可以先删除原来的索引数据
		importBuilder.setLastValueStorePath("mongodb_import");//记录上次采集的增量字段值的文件路径，作为下次增量（或者重启后）采集数据的起点，不同的任务这个路径要不一样
//		importBuilder.setLastValueStoreTableName("logs");//记录上次采集的增量字段值的表，可以不指定，采用默认表名increament_tab
//		importBuilder.setLastValueType(ImportIncreamentConfig.TIMESTAMP_TYPE);//指定字段类型：ImportIncreamentConfig.NUMBER_TYPE 数字类型,ImportIncreamentConfig.TIMESTAMP_TYPE为时间类型
		//设置增量起始值，long类型时间值
		try {
			Date date = format.parse("2000-01-01");
			importBuilder.setLastValue(date.getTime());
		}
		catch (Exception e){
			e.printStackTrace();
		}
		// 或者ImportIncreamentConfig.TIMESTAMP_TYPE 日期类型
		//增量配置结束

		//映射和转换配置开始
//		/**
//		 * 全局配置：mongodb-db  表字段名称到db 表字段的映射：比如document_id -> docId
//		 *
//		 */
//		importBuilder.addFieldMapping("docId","document_id")
//				.addFieldMapping("docwTime","docwtime")
//				.addIgnoreFieldMapping("channel_id");//添加忽略字段
//
//
//		/**
//		 * 全局配置：为每条记录添加额外的字段和值
//		 * 可以为基本数据类型，也可以是复杂的对象
//		 */
//		importBuilder.addFieldValue("testF1","f1value");
//		importBuilder.addFieldValue("testInt",0);
//		importBuilder.addFieldValue("testDate",new Date());
//		importBuilder.addFieldValue("testFormateDate","yyyy-MM-dd HH",new Date());
//		TestObject testObject = new TestObject();
//		testObject.setId("testid");
//		testObject.setName("jackson");
//		importBuilder.addFieldValue("testObject",testObject);
//
//		/**
//		 * 重新设置es数据结构
//		 */
		importBuilder.setDataRefactor(new DataRefactor() {
			/**
			 * 在记录级调整每条记录的数据：修改值，添加/去掉字段等，过滤掉符合特定条件的记录
			 * @param context
			 * @throws Exception
			 */
			public void refactor(Context context) throws Exception  {
				String id = context.getStringValue("_id");
				//如果
				if(id.equals("5dcaa59e9832797f100c6806"))
					context.setDrop(true);
				//添加字段extfiled2到记录中，值为2
				 context.addFieldValue("extfiled2",2);
				//添加字段extfiled到记录中，值为1
				context.addFieldValue("extfiled",1);
				boolean httpOnly = context.getBooleanValue("httpOnly");
				boolean secure = context.getBooleanValue("secure");
				//空值处理
				String userAccount = context.getStringValue("userAccount");
				if(userAccount == null)
					context.addFieldValue("userAccount","");
				//空值处理
				String testVO = context.getStringValue("testVO");
				if(testVO == null)
					context.addFieldValue("testVO","");
				//空值处理
				String privateAttr = context.getStringValue("privateAttr");
				if(privateAttr == null)
					context.addFieldValue("privateAttr","");
				//空值处理
				String local = context.getStringValue("local");
				if(local == null)
					context.addFieldValue("local","");


				//如果字段无需再放置到表中，可以忽略掉这些属性
//				context.addIgnoreFieldMapping("author");
//				context.addIgnoreFieldMapping("title");
//				context.addIgnoreFieldMapping("subtitle");
			}
		});
		//映射和转换配置结束

		/**
		 * 一次、作业创建一个内置的线程池，实现多线程并行数据导入DB功能，作业完毕后关闭线程池
		 */
		importBuilder.setParallel(true);//设置为多线程并行批量导入,false串行
		importBuilder.setQueue(10);//设置批量导入线程池等待队列长度
		importBuilder.setThreadCount(50);//设置批量导入线程池工作线程数量
		importBuilder.setContinueOnError(true);//任务出现异常，是否继续执行作业：true（默认值）继续执行 false 中断作业执行
		importBuilder.setAsyn(false);//true 异步方式执行，不等待所有导入作业任务结束，方法快速返回；false（默认值） 同步方式执行，等待所有导入作业任务结束，所有作业结束后方法才返回

		importBuilder.setDebugResponse(false);//设置是否将每次处理的reponse打印到日志文件中，默认false
		importBuilder.setDiscardBulkResponse(false);//设置是否需要批量处理的响应报文，不需要设置为false，true为需要，默认false
		importBuilder.setExportResultHandler(new ExportResultHandler<Object,Object>() {
			@Override
			public void success(TaskCommand<Object,Object> taskCommand, Object result) {
				System.out.println(result);
				System.out.println(taskCommand.getTaskMetrics());
			}

			@Override
			public void error(TaskCommand<Object,Object> taskCommand, Object result) {
				System.out.println(result);
				System.out.println(taskCommand.getTaskMetrics());
			}

			@Override
			public void exception(TaskCommand<Object,Object> taskCommand, Exception exception) {
				System.out.println(taskCommand.getTaskMetrics());
			}

			@Override
			public int getMaxRetry() {
				return 0;
			}
		});
		//
//		//设置任务执行拦截器，可以添加多个，定时任务每次执行的拦截器
//		importBuilder.addCallInterceptor(new CallInterceptor() {
//			@Override
//			public void preCall(TaskContext taskContext) {
//				System.out.println("preCall");
//			}
//
//			@Override
//			public void afterCall(TaskContext taskContext) {
//				System.out.println("afterCall");
//			}
//
//			@Override
//			public void throwException(TaskContext taskContext, Exception e) {
//				System.out.println("throwException");
//			}
//		}).addCallInterceptor(new CallInterceptor() {
//			@Override
//			public void preCall(TaskContext taskContext) {
//				System.out.println("preCall 1");
//			}
//
//			@Override
//			public void afterCall(TaskContext taskContext) {
//				System.out.println("afterCall 1");
//			}
//
//			@Override
//			public void throwException(TaskContext taskContext, Exception e) {
//				System.out.println("throwException 1");
//			}
//		});
//		//设置任务执行拦截器结束，可以添加多个
		/**
		 * 构建DataStream，执行mongodb数据到es的同步操作
		 */
		DataStream dataStream = importBuilder.builder();
		dataStream.execute();//执行同步操作

		System.out.println();
	}

}
