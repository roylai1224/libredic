/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <gtest/gtest.h>

#ifdef WIN32
#include <winsock2.h>
#define sleep(n) Sleep(n)
#else
#include <unistd.h>
#define sleep(n) usleep(n)
#endif

#include "redic.h"
string serverHost;
string serverPort;

#if 1

//test dataset operation
TEST(RedicTest, GeneralTest)
{
	redic_t rdc;
	string str;
    time_t time;

	ASSERT_EQ(redic_t::ok, rdc.connect(serverHost.c_str(), atoi(serverPort.c_str())));
	ASSERT_EQ(redic_t::ok, rdc.auth("redic"));

	ASSERT_EQ(redic_t::ok, rdc.info(str));
	ASSERT_EQ(redic_t::ok, rdc.ping());
    ASSERT_EQ(redic_t::ok, rdc.select(0));
    ASSERT_EQ(redic_t::ok, rdc.select(1));
    ASSERT_EQ(redic_t::ok, rdc.select(2));
    ASSERT_EQ(redic_t::ok, rdc.flushdb());
    ASSERT_EQ(redic_t::ok, rdc.save());
    ASSERT_EQ(redic_t::ok, rdc.bgsave());
    ASSERT_EQ(redic_t::ok, rdc.lastsave(time));

	SUCCEED();
}

//test key operation
TEST(RedicTest, KeyTest)
{
	redic_t rdc;
	string str;
    redic_t::List list;

	ASSERT_EQ(redic_t::ok, rdc.connect(serverHost.c_str(), atoi(serverPort.c_str())));
	ASSERT_EQ(redic_t::ok, rdc.auth("redic"));
    ASSERT_EQ(redic_t::ok, rdc.select(2));
    ASSERT_EQ(redic_t::ok, rdc.flushdb());

	ASSERT_EQ(redic_t::ok, rdc.set("key1", "val1"));
    ASSERT_EQ(redic_t::ok, rdc.randomkey(str));
    ASSERT_EQ("key1", str);

	ASSERT_EQ(redic_t::ok, rdc.set("key2", "val2"));
	ASSERT_EQ(2, rdc.dbsize());

	ASSERT_EQ(redic_t::ok, rdc.keys("key*", list));
	ASSERT_EQ(2, list.size());

	ASSERT_EQ(redic_t::ok, rdc.exists("key1"));
	ASSERT_EQ(redic_t::ok, rdc.type("key1", str));
    ASSERT_EQ("string", str);
    ASSERT_EQ(redic_t::ok, rdc.del("key1"));
    ASSERT_NE(redic_t::ok, rdc.exists("key1"));

	ASSERT_EQ(redic_t::ok, rdc.set("key1", "val1"));
	ASSERT_EQ(redic_t::ok, rdc.rename("key1", "keyx"));
	ASSERT_EQ(redic_t::ok, rdc.renamenx("keyx", "key1"));

#ifdef EXPIRE
	ASSERT_EQ(redic_t::ok, rdc.expire("key1", 1));
    sleep(2000);
	ASSERT_NE(redic_t::ok, rdc.exists("key1"));
#endif

    ASSERT_NE(redic_t::ok, rdc.exists("key?"));
	ASSERT_NE(redic_t::ok, rdc.type("key?", str));
    ASSERT_NE(redic_t::ok, rdc.rename("key1", "key1"));
    ASSERT_NE(redic_t::ok, rdc.renamenx("key1", "key1"));
	ASSERT_NE(redic_t::ok, rdc.renamenx("key1", "key2"));
    ASSERT_NE(redic_t::ok, rdc.rename("key?", "key2"));
    ASSERT_NE(redic_t::ok, rdc.renamenx("key?", "key2"));

	SUCCEED();
}

//test string operation
TEST(RedicTest, StringTest)
{
	redic_t rdc;
	string str;
    int val;
    redic_t::List list;

	ASSERT_EQ(redic_t::ok, rdc.connect(serverHost.c_str(), atoi(serverPort.c_str())));
	ASSERT_EQ(redic_t::ok, rdc.auth("redic"));
    ASSERT_EQ(redic_t::ok, rdc.select(2));
    ASSERT_EQ(redic_t::ok, rdc.flushdb());

	ASSERT_EQ(redic_t::ok, rdc.set("key1", "val1"));
	ASSERT_EQ(redic_t::ok, rdc.get("key1", str));
    ASSERT_EQ("val1", str);

	ASSERT_EQ(4, rdc.append("key2", "val2"));
	ASSERT_EQ(redic_t::ok, rdc.get("key2", str));
    ASSERT_EQ("val2", str);

	ASSERT_EQ(redic_t::ok, rdc.getset("key1", "valx", str));
    ASSERT_EQ("val1", str);
	ASSERT_EQ(redic_t::ok, rdc.getset("key1", "val1", str));
    ASSERT_EQ("valx", str);

#ifdef EXPIRE
	ASSERT_EQ(redic_t::ok, rdc.setex("key1", 1, "val1"));
    sleep(2000);
	ASSERT_NE(redic_t::ok, rdc.exists("key1"));
#endif

	ASSERT_EQ(redic_t::ok, rdc.set("key1", "val1"));
	ASSERT_NE(redic_t::ok, rdc.setnx("key1", "val1"));
	ASSERT_EQ(redic_t::ok, rdc.del("key1"));
	ASSERT_EQ(redic_t::ok, rdc.setnx("key1", "val1"));
	ASSERT_EQ(redic_t::ok, rdc.exists("key1"));
	ASSERT_EQ(redic_t::ok, rdc.get("key1", str));
    ASSERT_EQ("val1", str);

	ASSERT_EQ(4, rdc.strlen("key1"));
	ASSERT_EQ(0, rdc.strlen("key?"));

    list.push_back("key1");
    list.push_back("key2");
    ASSERT_EQ(redic_t::ok, rdc.mget(list, list));
	ASSERT_EQ("val1", *list.begin());
	ASSERT_EQ("val2", *list.rbegin());

	ASSERT_EQ(redic_t::ok, rdc.substr("key1", 0, 1, str));
    ASSERT_EQ("va", str);
	ASSERT_EQ(redic_t::ok, rdc.substr("key1", 1, 1, str));
    ASSERT_EQ("a", str);
	ASSERT_EQ(redic_t::ok, rdc.substr("key1", -2, -1, str));
    ASSERT_EQ("l1", str);

	ASSERT_EQ(redic_t::ok, rdc.incr("keyv", val));
    ASSERT_EQ(1, val);
	ASSERT_EQ(redic_t::ok, rdc.incrby("keyv", 10, val));
    ASSERT_EQ(11, val);
	ASSERT_EQ(redic_t::ok, rdc.decrby("keyv", 8, val));
    ASSERT_EQ(3, val);
	ASSERT_EQ(redic_t::ok, rdc.decr("keyv", val));
    ASSERT_EQ(2, val);
	ASSERT_EQ(redic_t::ok, rdc.decr("keyv", val));
    ASSERT_EQ(1, val);

	SUCCEED();
}

//test list operation
TEST(RedicTest, ListTest)
{
	redic_t rdc;
	string str;
    redic_t::List list;

	ASSERT_EQ(redic_t::ok, rdc.connect(serverHost.c_str(), atoi(serverPort.c_str())));
	ASSERT_EQ(redic_t::ok, rdc.auth("redic"));
    ASSERT_EQ(redic_t::ok, rdc.select(2));
    ASSERT_EQ(redic_t::ok, rdc.flushdb());

	ASSERT_EQ(1, rdc.rpush("keyl1", "val3"));
    ASSERT_EQ(2, rdc.rpushx("keyl1", "val4"));
    ASSERT_EQ(3, rdc.lpush("keyl1", "val2"));
    ASSERT_EQ(4, rdc.lpushx("keyl1", "val1"));
    ASSERT_EQ(4, rdc.llen("keyl1"));

    ASSERT_EQ(redic_t::ok, rdc.lpop("keyl1", str));
    ASSERT_EQ("val1", str);
    ASSERT_EQ(redic_t::ok, rdc.rpop("keyl1", str));
    ASSERT_EQ("val4", str);
    ASSERT_EQ(2, rdc.llen("keyl1"));
    ASSERT_EQ(3, rdc.rpush("keyl1", "val4"));
    ASSERT_EQ(4, rdc.lpush("keyl1", "val1"));

	ASSERT_EQ(redic_t::ok, rdc.lindex("keyl1", 1, str));
    ASSERT_EQ("val2", str);

	ASSERT_EQ(redic_t::ok, rdc.lset("keyl1", 2, "valx"));
	ASSERT_EQ(redic_t::ok, rdc.lindex("keyl1", 2, str));
    ASSERT_EQ("valx", str);

	ASSERT_EQ(redic_t::ok, rdc.lrange("keyl1", 0, 1, list));
    ASSERT_EQ(2, list.size());

	ASSERT_EQ(redic_t::ok, rdc.ltrim("keyl1", 0, 2));
    ASSERT_EQ(3, rdc.llen("keyl1"));

	SUCCEED();
}

//test set operation
TEST(RedicTest, SetTest)
{
	redic_t rdc;
	string str;
    redic_t::Set set;

	ASSERT_EQ(redic_t::ok, rdc.connect(serverHost.c_str(), atoi(serverPort.c_str())));
	ASSERT_EQ(redic_t::ok, rdc.auth("redic"));
    ASSERT_EQ(redic_t::ok, rdc.select(2));
    ASSERT_EQ(redic_t::ok, rdc.flushdb());

	ASSERT_EQ(redic_t::ok, rdc.sadd("keys1", "val1"));
	ASSERT_EQ(redic_t::ok, rdc.sadd("keys1", "valx"));
	ASSERT_EQ(redic_t::ok, rdc.sadd("keys1", "val2"));
	ASSERT_EQ(redic_t::ok, rdc.srem("keys1", "valx"));

	ASSERT_EQ(redic_t::ok, rdc.smove("keys1", "keys2", "val1"));
	ASSERT_EQ(redic_t::ok, rdc.smove("keys2", "keys1", "val1"));
	ASSERT_EQ(2, rdc.scard("keys1"));

	ASSERT_EQ(redic_t::ok, rdc.sismember("keys1", "val1"));
	ASSERT_NE(redic_t::ok, rdc.sismember("keys1", "valx"));

	ASSERT_EQ(redic_t::ok, rdc.sadd("keys2", "val2"));
	ASSERT_EQ(redic_t::ok, rdc.sadd("keys2", "val3"));
	ASSERT_EQ(redic_t::ok, rdc.sadd("keys3", "val2"));
	ASSERT_EQ(redic_t::ok, rdc.sadd("keys3", "val4"));

    set.clear();
    set.insert("keys1");
    set.insert("keys2");
    set.insert("keys3");
	ASSERT_EQ(redic_t::ok, rdc.sinter(set, set));
    ASSERT_EQ(1, set.size());

    set.clear();
    set.insert("keys1");
    set.insert("keys2");
    set.insert("keys3");
	ASSERT_EQ(1, rdc.sinterstore("keys4", set));
	ASSERT_EQ(1, rdc.scard("keys4"));

    set.clear();
    set.insert("keys1");
    set.insert("keys2");
    set.insert("keys3");
	ASSERT_EQ(redic_t::ok, rdc.sunion(set, set));
    ASSERT_EQ(4, set.size());

    set.clear();
    set.insert("keys1");
    set.insert("keys2");
    set.insert("keys3");
	ASSERT_EQ(4, rdc.sunionstore("keys4", set));
	ASSERT_EQ(4, rdc.scard("keys4"));

    set.clear();
    set.insert("keys1");
    set.insert("keys2");
    set.insert("keys3");
	ASSERT_EQ(redic_t::ok, rdc.sdiff(set, set));
    ASSERT_EQ(1, set.size());

    set.clear();
    set.insert("keys1");
    set.insert("keys2");
    set.insert("keys3");
	ASSERT_EQ(1, rdc.sdiffstore("keys4", set));
	ASSERT_EQ(1, rdc.scard("keys4"));

	ASSERT_EQ(redic_t::ok, rdc.smembers("keys1", set));
    ASSERT_EQ(2, set.size());

	ASSERT_EQ(redic_t::ok, rdc.srandmember("keys1", str));

	SUCCEED();
}

//test zset operation
TEST(RedicTest, ZsetTest)
{
	redic_t rdc;
    double val;
    redic_t::List list;

	ASSERT_EQ(redic_t::ok, rdc.connect(serverHost.c_str(), atoi(serverPort.c_str())));
	ASSERT_EQ(redic_t::ok, rdc.auth("redic"));
    ASSERT_EQ(redic_t::ok, rdc.select(2));
    ASSERT_EQ(redic_t::ok, rdc.flushdb());

	ASSERT_EQ(redic_t::ok, rdc.zadd("keyz1", 1.1, "val1"));
	ASSERT_EQ(redic_t::ok, rdc.zadd("keyz1", 1.2, "val2"));
	ASSERT_EQ(redic_t::ok, rdc.zadd("keyz1", 1.3, "val3"));
	ASSERT_EQ(redic_t::ok, rdc.zrem("keyz1", "val2"));
	ASSERT_EQ(2, rdc.zcard("keyz1"));
	ASSERT_EQ(redic_t::ok, rdc.zadd("keyz1", 2.1, "val2"));
	ASSERT_EQ(redic_t::ok, rdc.zincrby("keyz1", 1.7, "val3", val));
	ASSERT_EQ(3, val);
	ASSERT_EQ(0, rdc.zrank("keyz1", "val1"));
	ASSERT_EQ(2, rdc.zrank("keyz1", "val3"));
	ASSERT_EQ(2, rdc.zrevrank("keyz1", "val1"));
	ASSERT_EQ(0, rdc.zrevrank("keyz1", "val3"));

	ASSERT_EQ(redic_t::ok, rdc.zrange("keyz1", 0, 1, list));
	ASSERT_EQ("val1", *list.begin());
	ASSERT_EQ(redic_t::ok, rdc.zrevrange("keyz1", 0, 2, list));
	ASSERT_EQ("val3", *list.begin());

	ASSERT_EQ(redic_t::ok, rdc.zscore("keyz1", "val3", val));
	ASSERT_EQ(3, val);

	ASSERT_EQ(2, rdc.zcount("keyz1", 1, 2.5));

	ASSERT_EQ(redic_t::ok, rdc.zremrangebyscore("keyz1", 0.9, 1.3));
	ASSERT_EQ(2, rdc.zcard("keyz1"));
	ASSERT_EQ(redic_t::ok, rdc.zremrangebyrank("keyz1", 0, 0));
	ASSERT_EQ(1, rdc.zcard("keyz1"));

	SUCCEED();
}

//test hash operation
TEST(RedicTest, HashTest)
{
	redic_t rd;
	string str;
    int val;
    redic_t::List list;

	ASSERT_EQ(redic_t::ok, rd.connect(serverHost.c_str(), atoi(serverPort.c_str())));
	ASSERT_EQ(redic_t::ok, rd.auth("redic"));
    ASSERT_EQ(redic_t::ok, rd.select(2));
    ASSERT_EQ(redic_t::ok, rd.flushdb());

    ASSERT_EQ(redic_t::ok, rd.hset("keyh1", "fld1", "val1"));
    ASSERT_EQ(redic_t::ok, rd.hset("keyh1", "fld1", "val1"));
    ASSERT_EQ(redic_t::ok, rd.hsetnx("keyh1", "fld2", "val2"));
    ASSERT_NE(redic_t::ok, rd.hsetnx("keyh1", "fld2", "val2"));

    ASSERT_EQ(redic_t::ok, rd.hget("keyh1", "fld2", str));
    ASSERT_EQ("val2", str);

    list.push_back("fld3");
    list.push_back("val3");
    list.push_back("fld4");
    list.push_back("val4");
    ASSERT_EQ(redic_t::ok, rd.hmset("keyh1", list));

    list.clear();
    list.push_back("fld2");
    list.push_back("fld4");
    ASSERT_EQ(redic_t::ok, rd.hmget("keyh1", list, list));
    ASSERT_EQ("val2", *list.begin());
    ASSERT_EQ("val4", *list.rbegin());

    ASSERT_EQ(redic_t::ok, rd.hkeys("keyh1", list));
    ASSERT_EQ(4, list.size());

    ASSERT_EQ(redic_t::ok, rd.hvals("keyh1", list));
    ASSERT_EQ(4, list.size());

    ASSERT_EQ(redic_t::ok, rd.hgetall("keyh1", list));
    ASSERT_EQ(8, list.size());

    ASSERT_EQ(redic_t::ok, rd.hexists("keyh1", "fld3"));
    ASSERT_EQ(4, rd.hlen("keyh1"));

    ASSERT_NE(redic_t::ok, rd.hdel("keyh1", "fldx"));
    ASSERT_EQ(redic_t::ok, rd.hdel("keyh1", "fld3"));
    ASSERT_EQ(3, rd.hlen("keyh1"));

    ASSERT_EQ(redic_t::ok, rd.hset("keyh1", "fldv", "100"));
    ASSERT_EQ(redic_t::ok, rd.hincrby("keyh1", "fldv", 15, val));
    ASSERT_EQ(115, val);

	SUCCEED();
}

#endif

int main(int argc, char *argv[])
{
    if (argc != 3)
    {
        printf("Usage: test <Redis Server Host> <Redis Server Port>\r\n");
        return 0;
    }

    serverHost = argv[1];
    serverPort = argv[2];

    testing::InitGoogleTest(&argc, argv);
    RUN_ALL_TESTS();

	printf("press any key to continue...");
	getchar();
	return 0;
}

