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
import org.frameworkset.tran.CommonRecord;
import org.frameworkset.tran.DataRefactor;
import org.frameworkset.tran.DataStream;
import org.frameworkset.tran.ExportResultHandler;
import org.frameworkset.tran.config.ImportBuilder;
import org.frameworkset.tran.context.Context;
import org.frameworkset.tran.plugin.dummy.output.DummyOutputConfig;
import org.frameworkset.tran.plugin.mongodb.input.MongoDBInputConfig;
import org.frameworkset.tran.schedule.ImportIncreamentConfig;
import org.frameworkset.tran.schedule.TaskContext;
import org.frameworkset.tran.task.TaskCommand;
import org.frameworkset.tran.util.RecordGenerator;

import java.io.Writer;
import java.util.ArrayList;
import java.util.List;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2019/11/25 22:36
 * @author biaoping.yin
 * @version 1.0
 */
public class MongodbDate2Dummy {
	/**
	 * 启动运行同步作业主方法
	 * @param args
	 */
	public static void main(String[] args){

		MongodbDate2Dummy dbdemo = new MongodbDate2Dummy();
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
		mongoDBInputConfig.setName("useusu")
				.setDb("useusu")
				.setDbCollection("classes")
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

//		//定义mongodb数据查询条件对象（可选步骤，全量同步可以不需要做条件配置）
//		BasicDBObject query = new BasicDBObject();
//		// 设定检索mongdodb session数据时间范围条件
//		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
//		try {
//			Date start_date = format.parse("1099-01-01");
//			Date end_date = format.parse("2999-01-01");
//			query.append("creationTime",
//					new BasicDBObject("$gte", start_date.getTime()).append(
//							"$lte", end_date.getTime()));
//		}
//		catch (Exception e){
//			e.printStackTrace();
//		}
//		/**
//		// 设置按照host字段值进行正则匹配查找session数据条件（可选步骤，全量同步可以不需要做条件配置）
//		String host = "169.254.252.194-DESKTOP-U3V5C85";
//		Pattern hosts = Pattern.compile("^" + host + ".*$",
//				Pattern.CASE_INSENSITIVE);
//		query.append("host", new BasicDBObject("$regex",hosts));*/
//		mongoDBInputConfig.setQuery(query);

		//设定需要返回的session数据字段信息（可选步骤，同步全部字段时可以不需要做下面配置）
		List<String> fetchFields = new ArrayList<>();
		fetchFields.add("name");
		fetchFields.add("creationTime");
		fetchFields.add("members");


		mongoDBInputConfig.setFetchFields(fetchFields);
		importBuilder.setInputConfig(mongoDBInputConfig);
		// 5.2.4.3 导入dummy参数配置
		DummyOutputConfig dummyOupputConfig = new DummyOutputConfig();
		dummyOupputConfig.setRecordGenerator(new RecordGenerator() {
			@Override
			public void buildRecord(TaskContext taskContext, CommonRecord record, Writer builder) throws Exception{
				SimpleStringUtil.object2json(record.getDatas(),builder);

			}
		}).setPrintRecord(true);
		importBuilder.setOutputConfig(dummyOupputConfig);
		// 5.2.4.4 jdk timer定时任务时间配置（可选步骤，可以不需要做以下配置）
		importBuilder.setFixedRate(false)//参考jdk timer task文档对fixedRate的说明
//					 .setScheduleDate(date) //指定任务开始执行时间：日期
				.setDeyLay(1000L) // 任务延迟执行deylay毫秒后执行
				.setPeriod(5000L); //每隔period毫秒执行，如果不设置，只执行一次

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
		importBuilder.setLastValueColumn("creationTime");//手动指定数字增量查询字段
		importBuilder.setFromFirst(false);//任务重启时，重新开始采集数据，true 重新开始，false不重新开始，适合于每次全量导入数据的情况，如果是全量导入，可以先删除原来的索引数据
		importBuilder.setLastValueStorePath("mongodbdate_import");//记录上次采集的增量字段值的文件路径，作为下次增量（或者重启后）采集数据的起点，不同的任务这个路径要不一样
        importBuilder.setLastValueType(ImportIncreamentConfig.TIMESTAMP_TYPE);


		// 5.2.4.10 执行作业
		/**
		 * 构建DataStream，执行mongodb数据到dummy的同步操作
		 */
		DataStream dataStream = importBuilder.builder();
		dataStream.execute();//执行同步操作
	}
}
