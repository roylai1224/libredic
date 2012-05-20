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

typedef Redic::List List;
typedef Redic::Set Set;

static string serverHost;
static string serverPort;

#if 1

//test dataset operation
TEST(RedicTest, GeneralTest)
{
	Redic rdc;
	string val;
    time_t time;

	ASSERT_EQ(Redic::OK, rdc.connect(serverHost.c_str(), atoi(serverPort.c_str())));
	ASSERT_EQ(Redic::OK, rdc.auth("redic"));

	ASSERT_EQ(Redic::OK, rdc.info(val));
	ASSERT_EQ(Redic::OK, rdc.ping());
    ASSERT_EQ(Redic::OK, rdc.select(0));
    ASSERT_EQ(Redic::OK, rdc.select(1));
    ASSERT_EQ(Redic::OK, rdc.select(2));
    ASSERT_EQ(Redic::OK, rdc.flushdb());
    ASSERT_EQ(Redic::OK, rdc.save());
    ASSERT_EQ(Redic::OK, rdc.bgsave());
    ASSERT_EQ(Redic::OK, rdc.lastsave(time));

	SUCCEED();
}

//test key operation
TEST(RedicTest, KeyTest)
{
	Redic rdc;
	string val;
    List list;
    int size;

	ASSERT_EQ(Redic::OK, rdc.connect(serverHost.c_str(), atoi(serverPort.c_str())));
	ASSERT_EQ(Redic::OK, rdc.auth("redic"));
    ASSERT_EQ(Redic::OK, rdc.select(2));
    ASSERT_EQ(Redic::OK, rdc.flushdb());

	ASSERT_EQ(Redic::OK, rdc.set("key1", "val1"));
    ASSERT_EQ(Redic::OK, rdc.randomkey(val));
    ASSERT_EQ("key1", val);

	ASSERT_EQ(Redic::OK, rdc.set("key2", "val2"));
	ASSERT_EQ(Redic::OK, rdc.dbsize(size));
	ASSERT_EQ(2, size);

	ASSERT_EQ(Redic::OK, rdc.keys("key*", list));
	ASSERT_EQ(2, list.size());

	ASSERT_EQ(Redic::OK, rdc.exists("key1"));
	ASSERT_EQ(Redic::OK, rdc.type("key1", val));
    ASSERT_EQ("string", val);
    ASSERT_EQ(Redic::OK, rdc.del("key1"));
    ASSERT_NE(Redic::OK, rdc.exists("key1"));

	ASSERT_EQ(Redic::OK, rdc.set("key1", "val1"));
	ASSERT_EQ(Redic::OK, rdc.rename("key1", "keyx"));
	ASSERT_EQ(Redic::OK, rdc.renamenx("keyx", "key1"));

#ifdef EXPIRE
	ASSERT_EQ(Redic::OK, rdc.expire("key1", 1));
    sleep(2000);
	ASSERT_NE(Redic::OK, rdc.exists("key1"));
#endif

    ASSERT_NE(Redic::OK, rdc.exists("key?"));
	ASSERT_NE(Redic::OK, rdc.type("key?", val));
    ASSERT_NE(Redic::OK, rdc.rename("key1", "key1"));
    ASSERT_NE(Redic::OK, rdc.renamenx("key1", "key1"));
	ASSERT_NE(Redic::OK, rdc.renamenx("key1", "key2"));
    ASSERT_NE(Redic::OK, rdc.rename("key?", "key2"));
    ASSERT_NE(Redic::OK, rdc.renamenx("key?", "key2"));

	SUCCEED();
}

//test string operation
TEST(RedicTest, StringTest)
{
	Redic rdc;
	string val;
    List list;
    int len;

	ASSERT_EQ(Redic::OK, rdc.connect(serverHost.c_str(), atoi(serverPort.c_str())));
	ASSERT_EQ(Redic::OK, rdc.auth("redic"));
    ASSERT_EQ(Redic::OK, rdc.select(2));
    ASSERT_EQ(Redic::OK, rdc.flushdb());

	ASSERT_EQ(Redic::OK, rdc.set("key1", "val1"));
	ASSERT_EQ(Redic::OK, rdc.get("key1", val));
    ASSERT_EQ("val1", val);

	ASSERT_EQ(Redic::OK, rdc.append("key2", "val2", len));
	ASSERT_EQ(4, len);
	ASSERT_EQ(Redic::OK, rdc.get("key2", val));
    ASSERT_EQ("val2", val);

	ASSERT_EQ(Redic::OK, rdc.getset("key1", "valx", val));
    ASSERT_EQ("val1", val);
	ASSERT_EQ(Redic::OK, rdc.getset("key1", "val1", val));
    ASSERT_EQ("valx", val);

#ifdef EXPIRE
	ASSERT_EQ(Redic::OK, rdc.setex("key1", 1, "val1"));
    sleep(2000);
	ASSERT_NE(Redic::OK, rdc.exists("key1"));
#endif

	ASSERT_EQ(Redic::OK, rdc.set("key1", "val1"));
	ASSERT_NE(Redic::OK, rdc.setnx("key1", "val1"));
	ASSERT_EQ(Redic::OK, rdc.del("key1"));
	ASSERT_EQ(Redic::OK, rdc.setnx("key1", "val1"));
	ASSERT_EQ(Redic::OK, rdc.exists("key1"));
	ASSERT_EQ(Redic::OK, rdc.get("key1", val));
    ASSERT_EQ("val1", val);

	ASSERT_EQ(Redic::OK, rdc.strlen("key1", len));
	ASSERT_EQ(4, len);
	ASSERT_EQ(0, rdc.strlen("key?", len));
	ASSERT_EQ(0, len);

    list.push_back("key1");
    list.push_back("key2");
    ASSERT_EQ(Redic::OK, rdc.mget(list, list));
	ASSERT_EQ("val1", *list.begin());
	ASSERT_EQ("val2", *list.rbegin());

	ASSERT_EQ(Redic::OK, rdc.substr("key1", 0, 1, val));
    ASSERT_EQ("va", val);
	ASSERT_EQ(Redic::OK, rdc.substr("key1", 1, 1, val));
    ASSERT_EQ("a", val);
	ASSERT_EQ(Redic::OK, rdc.substr("key1", -2, -1, val));
    ASSERT_EQ("l1", val);

	SUCCEED();
}

//test string operation
TEST(RedicTest, IntegerTest)
{
	Redic rdc;
    int val;
    List list;

	ASSERT_EQ(Redic::OK, rdc.connect(serverHost.c_str(), atoi(serverPort.c_str())));
	ASSERT_EQ(Redic::OK, rdc.auth("redic"));
    ASSERT_EQ(Redic::OK, rdc.select(2));
    ASSERT_EQ(Redic::OK, rdc.flushdb());

	ASSERT_EQ(Redic::OK, rdc.incr("key1", val));
    ASSERT_EQ(1, val);
	ASSERT_EQ(Redic::OK, rdc.incrby("key1", 10, val));
    ASSERT_EQ(11, val);
	ASSERT_EQ(Redic::OK, rdc.decrby("key1", 8, val));
    ASSERT_EQ(3, val);
	ASSERT_EQ(Redic::OK, rdc.decr("key1", val));
    ASSERT_EQ(2, val);
	ASSERT_EQ(Redic::OK, rdc.decr("key1", val));
    ASSERT_EQ(1, val);

	SUCCEED();
}

//test list operation
TEST(RedicTest, ListTest)
{
	Redic rdc;
	string val;
    List list;
    int len;

	ASSERT_EQ(Redic::OK, rdc.connect(serverHost.c_str(), atoi(serverPort.c_str())));
	ASSERT_EQ(Redic::OK, rdc.auth("redic"));
    ASSERT_EQ(Redic::OK, rdc.select(2));
    ASSERT_EQ(Redic::OK, rdc.flushdb());

	ASSERT_EQ(Redic::OK, rdc.rpush("keyl1", "val3", len));
	ASSERT_EQ(1, len);
    ASSERT_EQ(Redic::OK, rdc.rpushx("keyl1", "val4", len));
	ASSERT_EQ(2, len);
    ASSERT_EQ(Redic::OK, rdc.lpush("keyl1", "val2", len));
	ASSERT_EQ(3, len);
    ASSERT_EQ(Redic::OK, rdc.lpushx("keyl1", "val1", len));
	ASSERT_EQ(4, len);
    ASSERT_EQ(Redic::OK, rdc.llen("keyl1", len));
	ASSERT_EQ(4, len);

    ASSERT_EQ(Redic::OK, rdc.lpop("keyl1", val));
    ASSERT_EQ("val1", val);
    ASSERT_EQ(Redic::OK, rdc.rpop("keyl1", val));
    ASSERT_EQ("val4", val);
    ASSERT_EQ(Redic::OK, rdc.llen("keyl1", len));
	ASSERT_EQ(2, len);
    ASSERT_EQ(Redic::OK, rdc.rpush("keyl1", "val4", len));
	ASSERT_EQ(3, len);
    ASSERT_EQ(Redic::OK, rdc.lpush("keyl1", "val1", len));
	ASSERT_EQ(4, len);

	ASSERT_EQ(Redic::OK, rdc.lindex("keyl1", 1, val));
    ASSERT_EQ("val2", val);

	ASSERT_EQ(Redic::OK, rdc.lset("keyl1", 2, "valx"));
	ASSERT_EQ(Redic::OK, rdc.lindex("keyl1", 2, val));
    ASSERT_EQ("valx", val);

	ASSERT_EQ(Redic::OK, rdc.lrange("keyl1", 0, 1, list));
    ASSERT_EQ(2, list.size());

	ASSERT_EQ(Redic::OK, rdc.ltrim("keyl1", 0, 2));
    ASSERT_EQ(Redic::OK, rdc.llen("keyl1", len));
	ASSERT_EQ(3, len);

	SUCCEED();
}

//test set operation
TEST(RedicTest, SetTest)
{
	Redic rdc;
	string val;
    Set set;
    int len;

	ASSERT_EQ(Redic::OK, rdc.connect(serverHost.c_str(), atoi(serverPort.c_str())));
	ASSERT_EQ(Redic::OK, rdc.auth("redic"));
    ASSERT_EQ(Redic::OK, rdc.select(2));
    ASSERT_EQ(Redic::OK, rdc.flushdb());

	ASSERT_EQ(Redic::OK, rdc.sadd("keys1", "val1"));
	ASSERT_EQ(Redic::OK, rdc.sadd("keys1", "valx"));
	ASSERT_EQ(Redic::OK, rdc.sadd("keys1", "val2"));
	ASSERT_EQ(Redic::OK, rdc.srem("keys1", "valx"));

	ASSERT_EQ(Redic::OK, rdc.smove("keys1", "keys2", "val1"));
	ASSERT_EQ(Redic::OK, rdc.smove("keys2", "keys1", "val1"));
	ASSERT_EQ(Redic::OK, rdc.scard("keys1", len));
	ASSERT_EQ(2, len);

	ASSERT_EQ(Redic::OK, rdc.sismember("keys1", "val1"));
	ASSERT_NE(Redic::OK, rdc.sismember("keys1", "valx"));

	ASSERT_EQ(Redic::OK, rdc.sadd("keys2", "val2"));
	ASSERT_EQ(Redic::OK, rdc.sadd("keys2", "val3"));
	ASSERT_EQ(Redic::OK, rdc.sadd("keys3", "val2"));
	ASSERT_EQ(Redic::OK, rdc.sadd("keys3", "val4"));

    set.clear();
    set.insert("keys1");
    set.insert("keys2");
    set.insert("keys3");
	ASSERT_EQ(Redic::OK, rdc.sinter(set, set));
    ASSERT_EQ(1, set.size());

    set.clear();
    set.insert("keys1");
    set.insert("keys2");
    set.insert("keys3");
	ASSERT_EQ(Redic::OK, rdc.sinterstore("keys4", set, len));
	ASSERT_EQ(1, len);
	ASSERT_EQ(Redic::OK, rdc.scard("keys4", len));
	ASSERT_EQ(1, len);

    set.clear();
    set.insert("keys1");
    set.insert("keys2");
    set.insert("keys3");
	ASSERT_EQ(Redic::OK, rdc.sunion(set, set));
    ASSERT_EQ(4, set.size());

    set.clear();
    set.insert("keys1");
    set.insert("keys2");
    set.insert("keys3");
	ASSERT_EQ(Redic::OK, rdc.sunionstore("keys4", set, len));
	ASSERT_EQ(4, len);
	ASSERT_EQ(Redic::OK, rdc.scard("keys4", len));
	ASSERT_EQ(4, len);

    set.clear();
    set.insert("keys1");
    set.insert("keys2");
    set.insert("keys3");
	ASSERT_EQ(Redic::OK, rdc.sdiff(set, set));
    ASSERT_EQ(1, set.size());

    set.clear();
    set.insert("keys1");
    set.insert("keys2");
    set.insert("keys3");
	ASSERT_EQ(Redic::OK, rdc.sdiffstore("keys4", set, len));
	ASSERT_EQ(1, len);
	ASSERT_EQ(Redic::OK, rdc.scard("keys4", len));
	ASSERT_EQ(1, len);

	ASSERT_EQ(Redic::OK, rdc.smembers("keys1", set));
    ASSERT_EQ(2, set.size());

	ASSERT_EQ(Redic::OK, rdc.srandmember("keys1", val));

	SUCCEED();
}

//test zset operation
TEST(RedicTest, ZsetTest)
{
	Redic rdc;
    double val;
    List list;
    int len;
    int rank;

	ASSERT_EQ(Redic::OK, rdc.connect(serverHost.c_str(), atoi(serverPort.c_str())));
	ASSERT_EQ(Redic::OK, rdc.auth("redic"));
    ASSERT_EQ(Redic::OK, rdc.select(2));
    ASSERT_EQ(Redic::OK, rdc.flushdb());

	ASSERT_EQ(Redic::OK, rdc.zadd("keyz1", 1.1, "val1"));
	ASSERT_EQ(Redic::OK, rdc.zadd("keyz1", 1.2, "val2"));
	ASSERT_EQ(Redic::OK, rdc.zadd("keyz1", 1.3, "val3"));
	ASSERT_EQ(Redic::OK, rdc.zrem("keyz1", "val2"));
	ASSERT_EQ(Redic::OK, rdc.zcard("keyz1", len));
	ASSERT_EQ(2, len);
	ASSERT_EQ(Redic::OK, rdc.zadd("keyz1", 2.1, "val2"));
	ASSERT_EQ(Redic::OK, rdc.zincrby("keyz1", 1.7, "val3", val));
	ASSERT_EQ(3, val);
	ASSERT_EQ(Redic::OK, rdc.zrank("keyz1", "val1", rank));
	ASSERT_EQ(0, rank);
	ASSERT_EQ(Redic::OK, rdc.zrank("keyz1", "val3", rank));
	ASSERT_EQ(2, rank);
	ASSERT_EQ(Redic::OK, rdc.zrevrank("keyz1", "val1", rank));
	ASSERT_EQ(2, rank);
	ASSERT_EQ(Redic::OK, rdc.zrevrank("keyz1", "val3", rank));
	ASSERT_EQ(0, rank);

	ASSERT_EQ(Redic::OK, rdc.zrange("keyz1", 0, 1, list));
	ASSERT_EQ("val1", *list.begin());
	ASSERT_EQ(Redic::OK, rdc.zrevrange("keyz1", 0, 2, list));
	ASSERT_EQ("val3", *list.begin());

	ASSERT_EQ(Redic::OK, rdc.zscore("keyz1", "val3", val));
	ASSERT_EQ(3, val);

	ASSERT_EQ(Redic::OK, rdc.zcount("keyz1", 1, 2.5, len));
	ASSERT_EQ(2, len);

	ASSERT_EQ(Redic::OK, rdc.zremrangebyscore("keyz1", 0.9, 1.3, len));
	ASSERT_EQ(1, len);
	ASSERT_EQ(Redic::OK, rdc.zcard("keyz1", len));
	ASSERT_EQ(2, len);
	ASSERT_EQ(Redic::OK, rdc.zremrangebyrank("keyz1", 0, 0, len));
	ASSERT_EQ(1, len);
	ASSERT_EQ(Redic::OK, rdc.zcard("keyz1", len));
	ASSERT_EQ(1, len);

	SUCCEED();
}

//test hash operation
TEST(RedicTest, HashTest1)
{
	Redic rd;
	string val;
    List list;
    int len;

	ASSERT_EQ(Redic::OK, rd.connect(serverHost.c_str(), atoi(serverPort.c_str())));
	ASSERT_EQ(Redic::OK, rd.auth("redic"));
    ASSERT_EQ(Redic::OK, rd.select(2));
    ASSERT_EQ(Redic::OK, rd.flushdb());

    ASSERT_EQ(Redic::OK, rd.hset("keyh1", "fld1", "val1"));
    ASSERT_EQ(Redic::OK, rd.hset("keyh1", "fld1", "val1"));
    ASSERT_EQ(Redic::OK, rd.hsetnx("keyh1", "fld2", "val2"));
    ASSERT_NE(Redic::OK, rd.hsetnx("keyh1", "fld2", "val2"));

    ASSERT_EQ(Redic::OK, rd.hget("keyh1", "fld2", val));
    ASSERT_EQ("val2", val);

    list.push_back("fld3");
    list.push_back("val3");
    list.push_back("fld4");
    list.push_back("val4");
    ASSERT_EQ(Redic::OK, rd.hmset("keyh1", list));

    list.clear();
    list.push_back("fld2");
    list.push_back("fld4");
    ASSERT_EQ(Redic::OK, rd.hmget("keyh1", list, list));
    ASSERT_EQ("val2", *list.begin());
    ASSERT_EQ("val4", *list.rbegin());

    ASSERT_EQ(Redic::OK, rd.hkeys("keyh1", list));
    ASSERT_EQ(4, list.size());

    ASSERT_EQ(Redic::OK, rd.hvals("keyh1", list));
    ASSERT_EQ(4, list.size());

    ASSERT_EQ(Redic::OK, rd.hgetall("keyh1", list));
    ASSERT_EQ(8, list.size());

    ASSERT_EQ(Redic::OK, rd.hexists("keyh1", "fld3"));
    ASSERT_EQ(Redic::OK, rd.hlen("keyh1", len));
    ASSERT_EQ(4, len);

    ASSERT_NE(Redic::OK, rd.hdel("keyh1", "fldx"));
    ASSERT_EQ(Redic::OK, rd.hdel("keyh1", "fld3"));
    ASSERT_EQ(Redic::OK, rd.hlen("keyh1", len));
    ASSERT_EQ(3, len);

	SUCCEED();
}

//test hash operation
TEST(RedicTest, HashTest2)
{
	Redic rd;
    int val;
    List list;
    int len;

	ASSERT_EQ(Redic::OK, rd.connect(serverHost.c_str(), atoi(serverPort.c_str())));
	ASSERT_EQ(Redic::OK, rd.auth("redic"));
    ASSERT_EQ(Redic::OK, rd.select(2));
    ASSERT_EQ(Redic::OK, rd.flushdb());

    ASSERT_EQ(Redic::OK, rd.hset("keyh1", "fldv", "100"));
    ASSERT_EQ(Redic::OK, rd.hincrby("keyh1", "fldv", 15, val));
    ASSERT_EQ(115, val);

    ASSERT_EQ(Redic::OK, rd.hlen("keyh1", len));
    ASSERT_EQ(1, len);

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

