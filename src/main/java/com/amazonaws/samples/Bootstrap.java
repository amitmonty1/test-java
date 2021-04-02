package com.amazonaws.samples;


import java.util.Hashtable;
import java.util.concurrent.atomic.AtomicLong;
import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Session;
import org.apache.activemq.ActiveMQSslConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.ibm.mq.MQQueue;
import com.ibm.mq.MQQueueManager;
import com.ibm.mq.constants.CMQC;
import com.ibm.mq.constants.MQConstants;


public class Bootstrap {

	public static void main(String... args) throws Exception {
		final AtomicLong amazonMQCounter = new AtomicLong();
		final AtomicLong ibmMQCounter = new AtomicLong();
		final Logger log = LoggerFactory.getLogger(Bootstrap.class);

		Thread xd = new Thread(new AmazonMQSender(System.getenv("amazonMQ.brokerURL"), "DEV.QUEUE.1",
				System.getenv("amazonMQ.userName"), System.getenv("amazonMQ.password"), amazonMQCounter));
       xd.start();
		long maxMessages = Long.parseLong(System.getenv("websphereMQ.maxMessage"));
		if(amazonMQCounter.longValue() == maxMessages){
			log.info("The counter value before the thread sleep is:"+amazonMQCounter.longValue());
			Thread.sleep(1000000);
		}
	//	new Thread(new IBMMQSender(System.getenv("websphereMQ.hostName"), System.getenv("websphereMQ.channel"),
		//		System.getenv("websphereMQ.queueManager"), "DEV.QUEUE.1", System.getenv("websphereMQ.userName"),
		//		System.getenv("websphereMQ.password"), ibmMQCounter)).start();

	}

	public static class AmazonMQSender implements Runnable {

		private Logger log = LoggerFactory.getLogger(AmazonMQSender.class);
		private ActiveMQSslConnectionFactory connFact;
		private Connection conn;
		private Session session;
		private MessageProducer messageProducer;
		private AtomicLong counter;
		private boolean exitflag = true;
		private long lStartTime = 0;
		private long lEndTime = 0;
			
        String msg1="<?xmlversion=1.0encoding=UTF-8?><projectxmlns=http://maven.apache.org/POM/4.0.0xmlns:xsi=http://www.w3.org/2001/XMLSchema-instancexsi:schemaLocation=http://maven.apache.org/POM/4.0.0http://maven.apache.org/maven-v4_0_0.xsd><modelVersion>4.0.0</modelVersion><version>1.0.0-SNAPSHOT</version><groupId>com.amazonaws.samples.amazon-mq-migration-from-ibm-mq</groupId><artifactId>load-generator</artifactId><packaging>jar</packaging><properties><aws-account-id>527926955031</aws-account-id><aws.region>us-east-1</aws.region><java.version>1.8</java.version><project.build.sourceEncoding>UTF-8</project.build.sourceEncoding><project.reporting.outputEncoding>UTF-8</project.reporting.outputEncoding><com.amazonaws.version>1.11.599</com.amazonaws.version><com.ibm.mq.version>9.1.0.0</com.ibm.mq.version><javax.jms.version>2.0.1</javax.jms.version><org.apache.camel.version>2.24.1</org.apache.camel.version><com.spotify.dockerfile-maven-plugin.version>1.4.10</com.spotify.dockerfile-maven-plugin.version><org.apache.maven.plugins.maven-compiler-plugin.version>3.8.1</org.apache.maven.plugins.maven-compiler-plugin.version><org.apache.maven.plugins.maven-deploy-plugin.version>2.8.2</org.apache.maven.plugins.maven-deploy-plugin.version><org.apache.maven.plugins.maven-shade-plugin.version>3.2.1</org.apache.maven.plugins.maven-shade-plugin.version></properties><dependencyManagement><dependencies><dependency><groupId>org.apache.camel</groupId><artifactId>camel-parent</artifactId><version>${org.apache.camel.version}</version><scope>import</scope><type>pom</type></dependency><dependency><groupId>javax.jms</groupId><artifactId>javax.jms-api</artifactId><version>${javax.jms.version}</version></dependency><dependency><groupId>com.ibm.mq</groupId><artifactId>com.ibm.mq.allclient</artifactId><version>${com.ibm.mq.version}</version></dependency></dependencies></dependencyManagement><dependencies><dependency><groupId>javax.jms</groupId><artifactId>javax.jms-api</artifactId></dependency><dependency><groupId>org.apache.activemq</groupId><artifactId>activemq-client</artifactId><exclusions><exclusion><groupId>org.apache.geronimo.specs</groupId><artifactId>geronimo-jms_1.1_spec</artifactId></exclusion></exclusions></dependency><dependency><groupId>com.ibm.mq</groupId><artifactId>com.ibm.mq.allclient</artifactId></dependency><dependency><groupId>org.apache.logging.log4j</groupId><artifactId>log4j-slf4j-impl</artifactId><scope>runtime</scope></dependency></dependencies><build><finalName>${project.artifactId}</finalName><plugins><plugin><groupId>org.apache.maven.plugins</groupId><artifactId>maven-compiler-plugin</artifactId><version>${org.apache.maven.plugins.maven-compiler-plugin.version}</version><configuration><source>${java.version}</source><target>${java.version}</target></configuration></plugin><plugin><groupId>org.apache.maven.plugins</groupId><artifactId>maven-shade-plugin</artifactId><version>${org.apache.maven.plugins.maven-shade-plugin.version}</version><configuration><createDependencyReducedPom>false</createDependencyReducedPom></configuration><executions><execution><phase>package</phase><goals><goal>shade</goal></goals><configuration><transformers><transformerimplementation=org.apache.maven.plugins.shade.resource.ManifestResourceTransformer><mainClass>com.amazonaws.samples.Bootstrap</mainClass></transformer></transformers><filters><filter><artifact>*:*</artifact><excludes><exclude>META-INF/*.SF</exclude><exclude>META-INF/*.DSA</exclude><exclude>META-INF/*.RSA</exclude></excludes></filter></filters></configuration></execution></executions></plugin></plugins></build></project>";
        String msg2="<?xmlversion=1.0encoding=UTF-8?><projectxmlns=http://maven.apache.org/POM/4.0.0xmlns:xsi=http://www.w3.org/2001/XMLSchema-instancexsi:schemaLocation=http://maven.apache.org/POM/4.0.0http://maven.apache.org/maven-v4_0_0.xsd><modelVersion>4.0.0</modelVersion><version>1.0.0-SNAPSHOT</version><groupId>com.amazonaws.samples.amazon-mq-migration-from-ibm-mq</groupId><artifactId>load-generator</artifactId><packaging>jar</packaging><properties><aws-account-id>527926955031</aws-account-id><aws.region>us-east-1</aws.region><java.version>1.8</java.version><project.build.sourceEncoding>UTF-8</project.build.sourceEncoding><project.reporting.outputEncoding>UTF-8</project.reporting.outputEncoding><com.amazonaws.version>1.11.599</com.amazonaws.version><com.ibm.mq.version>9.1.0.0</com.ibm.mq.version><javax.jms.version>2.0.1</javax.jms.version><org.apache.camel.version>2.24.1</org.apache.camel.version><com.spotify.dockerfile-maven-plugin.version>1.4.10</com.spotify.dockerfile-maven-plugin.version><org.apache.maven.plugins.maven-compiler-plugin.version>3.8.1</org.apache.maven.plugins.maven-compiler-plugin.version><org.apache.maven.plugins.maven-deploy-plugin.version>2.8.2</org.apache.maven.plugins.maven-deploy-plugin.version><org.apache.maven.plugins.maven-shade-plugin.version>3.2.1</org.apache.maven.plugins.maven-shade-plugin.version></properties><dependencyManagement><dependencies><dependency><groupId>org.apache.camel</groupId><artifactId>camel-parent</artifactId><version>${org.apache.camel.version}</version><scope>import</scope><type>pom</type></dependency><dependency><groupId>javax.jms</groupId><artifactId>javax.jms-api</artifactId><version>${javax.jms.version}</version></dependency><dependency><groupId>com.ibm.mq</groupId><artifactId>com.ibm.mq.allclient</artifactId><version>${com.ibm.mq.version}</version></dependency></dependencies></dependencyManagement><dependencies><dependency><groupId>javax.jms</groupId><artifactId>javax.jms-api</artifactId></dependency><dependency><groupId>org.apache.activemq</groupId><artifactId>activemq-client</artifactId><exclusions><exclusion><groupId>org.apache.geronimo.specs</groupId><artifactId>geronimo-jms_1.1_spec</artifactId></exclusion></exclusions></dependency><dependency><groupId>com.ibm.mq</groupId><artifactId>com.ibm.mq.allclient</artifactId></dependency><dependency><groupId>org.apache.logging.log4j</groupId><artifactId>log4j-slf4j-impl</artifactId><scope>runtime</scope></dependency></dependencies><build><finalName>${project.artifactId}</finalName><plugins><plugin><groupId>org.apache.maven.plugins</groupId><artifactId>maven-compiler-plugin</artifactId><version>${org.apache.maven.plugins.maven-compiler-plugin.version}</version><configuration><source>${java.version}</source><target>${java.version}</target></configuration></plugin><plugin><groupId>org.apache.maven.plugins</groupId><artifactId>maven-shade-plugin</artifactId><version>${org.apache.maven.plugins.maven-shade-plugin.version}</version><configuration><createDependencyReducedPom>false</createDependencyReducedPom></configuration><executions><execution><phase>package</phase><goals><goal>shade</goal></goals><configuration><transformers><transformerimplementation=org.apache.maven.plugins.shade.resource.ManifestResourceTransformer><mainClass>com.amazonaws.samples.Bootstrap</mainClass></transformer></transformers><filters><filter><artifact>*:*</artifact><excludes><exclude>META-INF/*.SF</exclude><exclude>META-INF/*.DSA</exclude><exclude>META-INF/*.RSA</exclude></excludes></filter></filters></configuration></execution></executions></plugin></plugins></build></project>";
        String msg3 = msg1 + msg2;
		public AmazonMQSender(String url, String destination, String user, String password, AtomicLong counter)
				throws JMSException {
			this.counter = counter;

			connFact = new ActiveMQSslConnectionFactory(url);
			connFact.setConnectResponseTimeout(10000);

			conn = connFact.createConnection(user, password);
			conn.start();

			session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

			messageProducer = session.createProducer(session.createQueue(destination));
			messageProducer.setDeliveryMode(DeliveryMode.PERSISTENT);
			// messageProducer.setTimeToLive(5 * 1000);
		}

		public void run() {
			while (exitflag) {
				try {
					long counterout = counter.incrementAndGet();
					messageProducer.send(session.createTextMessage("Message " + counterout+" "+msg3));
					
					if(counterout == 1){
						lStartTime = System.currentTimeMillis();
					}
					if(counterout == Integer.valueOf(System.getenv("AmazonMQ.maxMessage"))){
						lEndTime = System.currentTimeMillis();
						long output = lEndTime - lStartTime;
						log.info("Elapsed time in milliseconds to send 10000 messages to IBM MQ: " + output);
						exitflag = false;
					}

				}
				catch (Exception e) {
					log.info("Received exception {}", e);
				}
			}
		}
	}

	/*public static class IBMMQSender implements Runnable {

		private Logger log = LoggerFactory.getLogger(IBMMQSender.class);

		private AtomicLong counter;
        private boolean exitflagMQRead = true;
        MQQueue destQueue;
        MQQueueManager qMgr;
		long lStartTime1 = 0;
		long lEndTime1 = 0;     
		int openOptions = MQConstants.MQOO_INQUIRE;
		Hashtable<String,Object> props = new Hashtable<String,Object>(); 
		public IBMMQSender(String host, String channel, String queueManager, String destination, String user,
				String password, AtomicLong counter) throws JMSException {
			this.counter = counter;


            props.put(CMQC.CHANNEL_PROPERTY, channel);
            props.put(CMQC.HOST_NAME_PROPERTY, host);
            props.put(CMQC.PORT_PROPERTY, new Integer(1414));
            props.put(CMQC.USER_ID_PROPERTY, user);
            props.put(CMQC.PASSWORD_PROPERTY, password);
            //MQEnvironment.properties.put(MQC.TRANSPORT_PROPERTY,
            // MQC.TRANSPORT_MQSERIES);
            try{
            qMgr = new MQQueueManager(queueManager, props);
            destQueue = qMgr.accessQueue("DEV.QUEUE.1", openOptions);
            
            }catch(Exception ex){
            	log.info("Received exception in connecting to MQ Queue manager{}", ex);
            }
		}

		public void run() {
			while (exitflagMQRead) {
				try {
					try {
						long counterout1 = counter.incrementAndGet();
						if(counterout1 == 1){
							lStartTime1 = System.currentTimeMillis();
						}
						Thread.sleep(1000);
						
						int depth = destQueue.getCurrentDepth();
	                    //log.info("Qeue Depth is:"+depth+ " and evaluation count is :", counter.get());
	                    if(depth == Integer.valueOf(System.getenv("websphereMQ.maxMessage"))){
	                    	destQueue.close();
	                    	qMgr.disconnect();
	                        exitflagMQRead = false;
							lEndTime1 = System.currentTimeMillis();
							long output1 = lEndTime1 - lStartTime1;	
							log.info("Elapsed time in milliseconds to receive 10000 messages in IBM MQ: " + output1);
	                    }						
					} catch (InterruptedException e) {
						log.info("Thread interuppted in getting queue depth", e);
					}
					catch (Exception ex) {
						log.info("Exception in getting queue Depth{}", ex);
					}
					

				}catch (Exception e) {
					log.info("Received exception {}", e);
				}
			}
		}
	} */

}