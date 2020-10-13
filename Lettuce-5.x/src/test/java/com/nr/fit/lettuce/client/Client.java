package com.nr.fit.lettuce.client;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.newrelic.agent.introspec.DataStoreRequest;
import com.newrelic.agent.introspec.InstrumentationTestConfig;
import com.newrelic.agent.introspec.InstrumentationTestRunner;
import com.newrelic.agent.introspec.Introspector;
import com.newrelic.agent.introspec.TracedMetricData;
import com.newrelic.api.agent.Trace;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import redis.embedded.RedisServer;

//Let JUnit know about the test harness
@RunWith(InstrumentationTestRunner.class)
//Tell the test harness which classes are part of the instrumentation module
@InstrumentationTestConfig(includePrefixes = { "io.lettuce.core" })
public class Client {

	protected static RedisClient redisClient = null;
	protected static RedisServer redisServer = null;

	@BeforeClass
	public static void beforeClass() throws IOException {
		InputStream stream = Client.class.getResourceAsStream("/redis.properties");
		if (stream != null) {
			System.out.println("Opened inputstream to redis.properties");
		} else {
			System.out.println("Failed to open inputstream to redis.properties");
		}
		Properties properties = new Properties();
		if (stream != null) {
			properties.load(stream);
		}

		boolean useEmbedded = Boolean.parseBoolean(properties.getProperty("redis-server-embedded", "true"));

		String host = properties.getProperty("redis-server-host", "localhost");
		int port = Integer.parseInt(properties.getProperty("redis-server-port", "6379"));

		if (useEmbedded) {
			redisServer = new RedisServer(port);
			redisServer.start();
			System.out.println("Embedded Redis Server started on port " + port);
		} else {
			System.out.println("Will use external Redis server on " + host + " and port " + port);
		}
		try {
			Thread.sleep(5000L);
		} catch (Exception e) {
			e.printStackTrace();
		}
		redisClient = RedisClient.create("redis://" + host + ":" + port + "/0");
	}

	@Test
	public void doStringTest() {
		System.out.println("Performing StringTest");
		stringTest();
		Introspector introspector = InstrumentationTestRunner.getIntrospector();
		int finishedTransactionCount = introspector.getFinishedTransactionCount(5000);
		assertTrue(finishedTransactionCount == 1);
		String txnName = "OtherTransaction/Custom/com.nr.fit.lettuce.client.Client/stringTest";
		Collection<DataStoreRequest> dataStores = introspector.getDataStores(txnName);
		assertTrue(dataStores.size() == 3);
		introspector.clear();
		System.out.println("StringTest passed");
	}

	@Trace(dispatcher = true)
	public void stringTest() {
		StatefulRedisConnection<String, String> connection = redisClient.connect();
		RedisCommands<String, String> syncCommands = connection.sync();
		String key = "string-test";
		syncCommands.set(key, "OK");
		syncCommands.append(key, "X");
		syncCommands.get(key);
		connection.close();
	}

	@Test
	public void doSetTest() {
		System.out.println("Performing SetTest");
		setTest();

		Introspector introspector = InstrumentationTestRunner.getIntrospector();
		int finishedTransactionCount = introspector.getFinishedTransactionCount(5000);
		assertTrue(finishedTransactionCount == 1);
		String txnName = "OtherTransaction/Custom/com.nr.fit.lettuce.client.Client/setTest";
		Map<String, TracedMetricData> metrics = introspector.getMetricsForTransaction(txnName);
		Set<String> keys = metrics.keySet();
		assertTrue(keys.contains("Datastore/statement/Redis/?/SPOP"));
		assertTrue(keys.contains("Datastore/statement/Redis/?/SMEMBERS"));
		assertTrue(keys.contains("Datastore/statement/Redis/?/SADD"));

		introspector.clear();

		System.out.println("SetTest passed");
	}

	@Trace(dispatcher = true)
	public void setTest() {
		StatefulRedisConnection<String, String> connection = redisClient.connect();
		RedisCommands<String, String> syncCommands = connection.sync();
		String key = "key-Set";
		syncCommands.sadd(key, "a");
		syncCommands.smembers(key);
		syncCommands.sadd(key, "a", "b", "c");
		String rand = syncCommands.spop(key);
		System.out.println("rand: " + rand);

		connection.close();
	}

	@Test
	public void doHashTest() {
		System.out.println("Performing HashTest");
		hashTest();
		String call1 = "Datastore/statement/Redis/?/HSET";
		String call2 = "Datastore/statement/Redis/?/HDEL";
		Introspector introspector = InstrumentationTestRunner.getIntrospector();
		int finishedTransactionCount = introspector.getFinishedTransactionCount(5000);
		assertTrue(finishedTransactionCount == 1);
		String txnName = "OtherTransaction/Custom/com.nr.fit.lettuce.client.Client/hashTest";
		Map<String, TracedMetricData> metrics = introspector.getMetricsForTransaction(txnName);
		Set<String> keys = metrics.keySet();
		assertTrue(keys.contains(call1));
		assertTrue(keys.contains(call2));
		System.out.println("HashTest passed");
	}

	@Trace(dispatcher = true)
	public void hashTest() {
		StatefulRedisConnection<String, String> connection = redisClient.connect();
		RedisCommands<String, String> syncCommands = connection.sync();
		String key = "key-hash";
		syncCommands.hdel(key, "one");
		syncCommands.hset(key, "one", "1");
		syncCommands.hdel(key, "one");

	}

	@Test
	public void doDoSetTest() {
		doSet();
		Introspector introspector = InstrumentationTestRunner.getIntrospector();
		int finishedTransactionCount = introspector.getFinishedTransactionCount(5000);
		assertTrue(finishedTransactionCount == 1);
		String txnName = "OtherTransaction/Custom/com.nr.fit.lettuce.client.Client/doSet";
		Collection<DataStoreRequest> dataStores = introspector.getDataStores(txnName);
		assertTrue(dataStores.size() == 1);
		DataStoreRequest[] requests = new DataStoreRequest[dataStores.size()];
		dataStores.toArray(requests);
		DataStoreRequest request = requests[0];
		assertTrue(request.getCount() == 1 && request.getDatastore().equalsIgnoreCase("redis")
				&& request.getOperation().equalsIgnoreCase("set"));
		introspector.clear();
	}

	@Trace(dispatcher = true)
	public void doSet() {

		StatefulRedisConnection<String, String> connection = redisClient.connect();
		RedisCommands<String, String> syncCommands = connection.sync();

		syncCommands.set("key", "Hello, Redis!");

		connection.close();
	}

	@Test
	public void doReadWriteTest() {
		readwrite();
		Introspector introspector = InstrumentationTestRunner.getIntrospector();
		int finishedTransactionCount = introspector.getFinishedTransactionCount(5000);
		assertTrue(finishedTransactionCount == 1);
		String txnName = "OtherTransaction/Custom/com.nr.fit.lettuce.client.Client/readwrite";
		Collection<DataStoreRequest> dataStores = introspector.getDataStores(txnName);
		assertTrue(dataStores.size() == 2);
		for (DataStoreRequest request : dataStores) {
			assertTrue(request.getCount() == 1 && request.getDatastore().equalsIgnoreCase("redis")
					&& (request.getOperation().equalsIgnoreCase("set")
							|| request.getOperation().equalsIgnoreCase("get")));
		}
		introspector.clear();
	}

	@Trace(dispatcher = true)
	public void readwrite() {
		StatefulRedisConnection<String, String> connection = redisClient.connect();

		System.out.println("Connected to Redis");

		RedisCommands<String, String> sync = connection.sync();

		sync.set("foo", "bar");
		String value = sync.get("foo");
		System.out.println(value);

		connection.close();
	}

	@AfterClass
	public static void shutdown() {
		if (redisServer != null) {
			redisServer.stop();
		}
		if (redisClient != null) {
			redisClient.shutdown();
		}
	}

}
