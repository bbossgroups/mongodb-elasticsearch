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
import com.mongodb.DBObject;
import org.frameworkset.elasticsearch.ElasticSearchHelper;
import org.frameworkset.elasticsearch.client.ClientInterface;
import org.frameworkset.runtime.CommonLauncher;
import org.frameworkset.session.TestVO;
import org.frameworkset.soa.ObjectSerializable;
import org.frameworkset.spi.geoip.IpInfo;
import org.frameworkset.tran.DataRefactor;
import org.frameworkset.tran.DataStream;
import org.frameworkset.tran.ExportResultHandler;
import org.frameworkset.tran.context.Context;
import org.frameworkset.tran.mongodb.input.es.MongoDB2ESExportBuilder;
import org.frameworkset.tran.task.TaskCommand;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2019/11/25 22:36
 * @author biaoping.yin
 * @version 1.0
 */
public class Mongodb2ES {
	/**
	 * 启动运行同步作业主方法
	 * @param args
	 */
	public static void main(String[] args){

		Mongodb2ES dbdemo = new Mongodb2ES();
		dbdemo.scheduleImportData();
	}

	/**
	 * 同步作业实现和运行方法
	 */
	public void scheduleImportData(){
		// 5.2.1 清理Elasticsearch索引表mongodbdemo(可选步骤)
		//从application.properties配置文件中读取dropIndice属性，
		// 是否清除elasticsearch索引，true清除，false不清除，指定了默认值false
		boolean dropIndice = CommonLauncher.getBooleanAttribute("dropIndice",true);
		ClientInterface clientInterface = ElasticSearchHelper.getRestClientUtil();
		//增量定时任务不要删表，但是可以通过删表来做初始化操作
		if(dropIndice) {
			try {
				//清除测试表,导入的时候自动重建表，测试的时候加上为了看测试效果，实际线上环境不要删表
				String repsonse = clientInterface.dropIndice("mongodbdemo");
				System.out.println(repsonse);
			} catch (Exception e) {
			}
		}
		// 5.2.1 创建elasticsearch index mapping(可选步骤)
		//判断mongodbdemo是否存在，如果不存在则创建mongodbdemo
		boolean indiceExist = clientInterface.existIndice("mongodbdemo");
		ClientInterface configClientInterface = ElasticSearchHelper.getConfigRestClientUtil("dsl.xml");
		if(!indiceExist){

			configClientInterface.createIndiceMapping("mongodbdemo","createMongodbdemoIndice");
		}
		// 5.2.3 创建elasticsearch index template(可选步骤)
		String template = clientInterface.getTempate("mongodbdemo_template");
		if(template == null){
			configClientInterface.createTempate("mongodbdemo_template","createMongodbdemoTemplate");
		}
		// 5.2.4 编写同步代码
		//定义Mongodb到Elasticsearch数据同步组件
		MongoDB2ESExportBuilder importBuilder = MongoDB2ESExportBuilder.newInstance();

		// 5.2.4.1 设置mongodb参数
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
		importBuilder.setQuery(query);

		//设定需要返回的session数据字段信息（可选步骤，同步全部字段时可以不需要做下面配置）
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
		fetchFields.put("shardNo", 1);

		importBuilder.setFetchFields(fetchFields);
		// 5.2.4.3 导入elasticsearch参数配置
		importBuilder
				.setIndex("mongodbdemo") //必填项，索引名称
				.setIndexType("mongodbdemo") //es 7以后的版本不需要设置indexType，es7以前的版本必需设置indexType
//				.setRefreshOption("refresh")//可选项，null表示不实时刷新，importBuilder.setRefreshOption("refresh");表示实时刷新
				.setPrintTaskLog(true) //可选项，true 打印任务执行日志（耗时，处理记录数） false 不打印，默认值false
				.setBatchSize(10)  //可选项,批量导入es的记录数，默认为-1，逐条处理，> 0时批量处理
				.setFetchSize(100)  //按批从mongodb拉取数据的大小
		        .setEsIdField("_id")//设置文档主键，不设置，则自动产生文档id,直接将mongodb的ObjectId设置为Elasticsearch的文档_id
				.setContinueOnError(true); // 忽略任务执行异常，任务执行过程抛出异常不中断任务执行

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
		importBuilder.setAsyn(false);//是否同步等待每批次任务执行完成后再返回调度程序，true 不等待所有导入作业任务结束，方法快速返回；false（默认值） 等待所有导入作业任务结束，所有作业结束后方法才返回

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
				DBObject record = (DBObject) context.getRecord();
			}
		});

		// 5.2.4.7 设置同步作业结果回调处理函数（可选步骤，可以不需要做以下配置）
		//设置任务处理结果回调接口
		importBuilder.setExportResultHandler(new ExportResultHandler<Object,String>() {
			@Override
			public void success(TaskCommand<Object,String> taskCommand, String result) {
				System.out.println(taskCommand.getTaskMetrics());//打印任务执行情况
			}

			@Override
			public void error(TaskCommand<Object,String> taskCommand, String result) {
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
			public void exception(TaskCommand<Object,String> taskCommand, Exception exception) {
				System.out.println(taskCommand.getTaskMetrics());//打印任务执行情况
			}

			@Override
			public int getMaxRetry() {
				return 0;
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
		importBuilder.setNumberLastValueColumn("lastAccessedTime");//手动指定数字增量查询字段
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
		 * 构建DataStream，执行mongodb数据到es的同步操作
		 */
		DataStream dataStream = importBuilder.builder();
		dataStream.execute();//执行同步操作
	}
}