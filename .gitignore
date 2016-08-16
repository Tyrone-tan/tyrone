package com.uu.business.activity.jodis;

import java.util.List;

import redis.clients.jedis.Jedis;

import com.uu.common.jodis.CodisNameSpace;
import com.uu.common.jodis.JodisClient;
import com.uu.common.jodis.JodisClient.JKey;
import com.uu.common.jodis.JodisClient.RedisCmd;


public class JodisCommandClient {

	private JodisClient jodisClient;
	
	public void setJodisClient(JodisClient jodisClient) {
		this.jodisClient = jodisClient;
	}
	
	/**
	 * MethodName:del
	 * description: 删除
	 * date: 2016年5月5日 上午11:48:50
	 * @author tanxiaolei
	 * @param nameSpace	命名空间
	 * @param key	key
	 * @return
	 */
	public Long del(CodisNameSpace nameSpace, String key) {
		return jodisClient.exe(nameSpace, key, new RedisCmd<Long>() {
			@Override
			public Long exe(Jedis jds, JKey key) {
				return jds.del(key.sKey());
			}
		});
	}
	
	/**
	 * MethodName:set
	 * description: 设置key-value并设置过期时间(单位：秒)
	 * date: 2016年5月5日 上午11:49:40
	 * @author tanxiaolei
	 * @param nameSpace
	 * @param key
	 * @param value byte数组
	 * @param seconds
	 * @return
	 */
	public String set(CodisNameSpace nameSpace, String key, final byte[] value, final int seconds) {
		return jodisClient.exe(nameSpace, key, new RedisCmd<String>() {
			@Override
			public String exe(Jedis jds, JKey key) {
				return jds.setex(key.bKey(), seconds, value);
			}
		});
	}
	
	public byte[] get(CodisNameSpace nameSpace, String key) {
		return jodisClient.exe(nameSpace, key, new RedisCmd<byte[]>() {
			@Override
			public byte[] exe(Jedis jds, JKey key) {
				return jds.get(key.bKey());
			}
		});
	}
	
	public String set(CodisNameSpace nameSpace, String key, final String value, final int seconds) {
		return jodisClient.exe(nameSpace, key, new RedisCmd<String>() {
			@Override
			public String exe(Jedis jds, JKey key) {
				return jds.setex(key.sKey(), seconds, value);
			}
		});
	}
	
	public String getStr(CodisNameSpace nameSpace, String key) {
		return jodisClient.exe(nameSpace, key, new RedisCmd<String>() {
			@Override
			public String exe(Jedis jds, JKey key) {
				return jds.get(key.sKey());
			}
		});
	}
	
	public String hget(CodisNameSpace nameSpace, String key, final String field) {
		return jodisClient.exe(nameSpace, key, new RedisCmd<String>() {
			@Override
			public String exe(Jedis jds, JKey key) {
				return jds.hget(key.sKey(), field);
			}
		});
	}
	
	public Long hset(CodisNameSpace nameSpace, String key, final String field, final String value) {
		return jodisClient.exe(nameSpace, key, new RedisCmd<Long>() {
			@Override
			public Long exe(Jedis jds, JKey key) {
				return jds.hset(key.sKey(), field, value);
			}
		});
	}
	
	public Long hdel(CodisNameSpace nameSpace, String key, final String... field) {
		return jodisClient.exe(nameSpace, key, new RedisCmd<Long>() {
			@Override
			public Long exe(Jedis jds, JKey key) {
				return jds.hdel(key.sKey(), field);
			}
		});
	}

	/**
	 * MethodName:incr
	 * description: 执行原子加1操作
	 * date: 2016年5月5日 下午12:02:40
	 * @author tanxiaolei
	 * @param nameSpace
	 * @param key
	 * @return
	 */
	public Long incr(CodisNameSpace nameSpace, String key){
		return jodisClient.exe(nameSpace, key, new RedisCmd<Long>(){
			@Override
			public Long exe(Jedis jds, JKey key) {
				return jds.incr(key.sKey());
			}
		});
	}
	
	/**
	 * MethodName:keyExists
	 * description: 查询一个key是否存在
	 * date: 2016年5月5日 下午12:10:59
	 * @author tanxiaolei
	 * @param nameSpace
	 * @param key
	 * @return
	 */
	public boolean keyExists(CodisNameSpace nameSpace, String key){
		return jodisClient.exe(nameSpace, key, new RedisCmd<Boolean>(){
			@Override
			public Boolean exe(Jedis jds, JKey key) {
				return jds.exists(key.sKey());
			}
		});
	}
	
	/**
	 * MethodName:expire
	 * description: 设置一个key的过期的秒数
	 * date: 2016年5月5日 下午12:15:57
	 * @author tanxiaolei
	 * @param nameSpace
	 * @param key
	 * @param seconds 秒数
	 * @return
	 */
	public Long expire(CodisNameSpace nameSpace, String key, final int seconds){
		return jodisClient.exe(nameSpace, key, new RedisCmd<Long>(){
			@Override
			public Long exe(Jedis jds, JKey key) {
				return jds.expire(key.sKey(), seconds);
			}
		});
	}
	
	/**
	 * MethodName:lrange
	 * description: 从列表中获取指定返回的元素
	 * date: 2016年5月5日 下午2:36:09
	 * @author tanxiaolei
	 * @param nameSpace
	 * @param key
	 * @param start  list元素下标
	 * @param end	 list元素下标 (-1 表示列表的最后一个元素，-2 是倒数第二个)
	 * @return
	 */
	public List<byte[]> lrange(CodisNameSpace nameSpace, String key, final int start, final int end){
		return jodisClient.exe(nameSpace, key, new RedisCmd<List<byte[]>>(){
			@Override
			public List<byte[]> exe(Jedis jds, JKey key) {
				return jds.lrange(key.bKey(), start, end);
			}
		});
	}
	
	/**
	 * MethodName:leftPush
	 * description: 从队列左边入队一个元素
	 * date: 2016年5月5日 下午3:05:17
	 * @author tanxiaolei
	 * @param nameSpace
	 * @param key
	 * @param value
	 * @return
	 */
	public Long leftPush(CodisNameSpace nameSpace, String key, final byte[] value){
		return jodisClient.exe(nameSpace, key, new RedisCmd<Long>(){
			@Override
			public Long exe(Jedis jds, JKey key) {
				return jds.lpush(key.bKey(), value);
			}
		});
	}
}
