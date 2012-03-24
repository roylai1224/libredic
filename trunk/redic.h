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

#ifndef _REDIC_H_
#define _REDIC_H_

#include <list>
#include <set>
#include <string>
using std::string;
class redic_entity_t;


#ifndef LENGTH_MAX
#define LENGTH_MAX 64000
#endif

#ifndef TIMEOUT_VAL
#define TIMEOUT_VAL 1000
#endif


class redic_t
{
public:
	typedef std::list<string> List;
	typedef std::set<string> Set;
	enum { ok = 1 };

	redic_t();
	~redic_t();
	const char *errstr(int num);

    ///Connect to Redis server, Return ok if succeed.
	int connect(const char *host, short port);

	///Disconnect from Redis server.
	void disconn();


    /* dataset operation */

	///Request for authentication in a password protected Redis server.
    int auth(const char *password);

	///Request for different information and statistics about the server.
	int info(string &info);

    ///Test if a connection is still alive, or measure network latency.
    int ping();

	///Synchronously save the dataset to disk.
    int save();

	///Asynchronously save the dataset to disk.
	int bgsave();

	///Request the UNIX TIME of the last save executed with success.
	int lastsave(time_t &tm);

	///Rewrite the append-only file to disk.
	int bgrewriteaof();

	///Select the dataset with having the specified zero-based numeric index.
	int select(int index);

	///Get a random key from the currently dataset.
	int randomkey(string &key);

    ///Return the number of keys in the currently dataset.
    int dbsize();

    ///Delete all the keys of the currently dataset.
    int flushdb();

	///Delete all the keys of all the existing dataset
    int flushall();

	///Get all keys matching pattern.
	int keys(const char *pattern, List &keys);


    /* key operation */

    ///Return ok if a key exists
    int exists(const char *key);

    ///Remove the specified keys.
    int del(const char *key);

    ///Get the representation of the type of the key.
    ///The types: string, list, set, zset and hash.
    int type(const char *key, string &type);

    ///Rename key to newkey. If newkey already exists it is overwritten.
    int rename(const char *key, const char *newkey);

    ///Rename key to newkey if newkey does not yet exist.
    int renamenx(const char *key, const char *newkey);

	///Set a timeout on key. After expired, the key will be deleted.
	int expire(const char *key, int secs);

	///Return the remaining time to live of a key that has a timeout.
	int ttl(const char *key);

	///Move key from the currently dataset to another dataset.
	int move(const char *key, int index);


	/* string operation */

    ///Append the value at the end of the string, and return the length of string.
    int append(const char *key, const char *value);

	///Set key to hold the string value.
	int set(const char *key, const char *value);

	///Get the string value of key.
	int get(const char *key, string &value);

	///Atomically set key to hold the string value and get the old string value.
	int getset(const char *key, const char *value, string &old_val);

	///Set key to hold the string value and set a timeout on key.
	int setex(const char *key, int secs, const char *value);

	///Set key to hold string value if key does not exist.
	int setnx(const char *key, const char *value);

    ///Return the length of the string value stored at key.
	int strlen(const char *key);

	///Get the substring of the string value stored at key.
	int substr(const char *key, int start, int end, string &value);

	///Get the string values of all specified keys.
	int mget(const List &keys, List &values);

	///Increment the number stored at key by one, and Get the value after increment
	int incr(const char *key, int &new_val);

	///Increment the number stored at key by increment.
	int incrby(const char *key, int increment, int &new_val);

	///Decrement the number stored at key by one.
	int decr(const char *key, int &new_val);

	///Decrement the number stored at key by decrement.
	int decrby(const char *key, int decrement, int &new_val);


	/* list operation */

    ///Insert element at the tail of the list stored at key.
    ///Return the length of the list after the push operation.
	int rpush(const char *key, const char *element);

	///Insert element at the tail of the list stored at key if key already exists and holds a list.
    ///Return the length of the list after the push operation.
	int rpushx(const char *key, const char *element);

    ///Insert element at the head of the list stored at key.
    ///Return the length of the list after the push operation.
    int lpush(const char *key, const char *element);

    ///Insert element at the head of the list stored at key if key already exists and holds a list.
    ///Return the length of the list after the push operation.
	int lpushx(const char *key, const char *element);

	///Remove and return the first element of the list stored at key.
	int lpop(const char *key, string &element);

	///Remove and return the last element of the list stored at key.
	int rpop(const char *key, string &element);

    ///Return the length of the list stored at key.
	int llen(const char *key);

    ///Get the specified elements of the list stored at key.
	int lrange(const char *key, int start, int range, List &elements);

    ///Trim an existing list to contain only the specified range of elements.
	int ltrim(const char *key, int start, int end);

    ///Set the list element at index to value.
	int lset(const char *key, int index, const char *element);

    ///Get the element at index in the list stored at key.
	int lindex(const char *key, int index, string &element);

	///Remove the first count occurrences of elements from the list stored at key.
    ///Return the number of removed elements.
	int lrem(const char *key, int count, const char *element);


	/* set operation */

    ///Add member to the set stored at key.
    int sadd(const char *key, const char *member);

    ///Remove member from the set stored at key.
    int srem(const char *key, const char *member);

    ///Remove and return a random element from the set value stored at key.
    int spop(const char *key, string &value);

    ///Move member from the set at source to the set at destination.
	int smove(const char *srckey, const char *destkey, const char *member);

	///Return the number of members of the set stored at key.
	int scard(const char *key);

	///Make sure if member is a member of the set stored at key.
	int sismember(const char *key, const char *member);

	///Get the members of the intersection of all the given sets.
	int sinter(const Set &keys, Set &members);

    ///Store the members of the intersection in destination set.
	int sinterstore(const char *destkey, const Set &keys);

    ///Get the members of the union of all the given sets.
	int sunion(const Set &keys, Set &members);

    ///Store the members of the union in destination set.
	int sunionstore(const char *destkey, const Set &keys);

    ///Get the members of the difference between the first set and all the successive sets.
	int sdiff(const Set &keys, Set &members);

    ///Store the members of the difference in destination set.
	int sdiffstore(const char *destkey, const Set &keys);

    ///Get all the members of the set value stored at key.
	int smembers(const char *key, Set &members);

    ///Get a random element from the set value stored at key.
	int srandmember(const char *key, string &member);


	/* zset (sorted set) operation */

    ///Add the member with the specified score to the sorted set stored at key.
	int zadd(const char *key, double score, const char *member);

	///Remove the member from the sorted set stored at key.
	int zrem(const char *key, const char *member);

	///Increment the score of member in the sorted set stored at key by increment.
	int zincrby(const char *key, double increment, const char *member, double &new_score);

	///Return the rank of member in the sorted set stored at key (rank 0 with lowest score).
	int zrank(const char *key, const char *member);

    ///Return the rank of member in the sorted set stored at key (rank 0 with highest score).
	int zrevrank(const char *key, const char *member);

	///Get the specified range of elements in the sorted set stored at key.
	int zrange(const char *key, int start, int stop, List &elements);

	///Get the specified range of elements in the sorted set stored at key.
	int zrevrange(const char *key, int start, int stop, List &elements);

	///Return the sorted set cardinality of the sorted set stored at key.
	int zcard(const char *key);

	///Get the score of member in the sorted set at key.
	int zscore(const char *key, const char *member, double &score);

	///Remove all elements in the sorted set with rank between start and stop.
	int zremrangebyscore(const char *key, double min, double max);

	///Remove all elements in the sorted set with score between min and max.
	int zremrangebyrank(const char *key, int start, int end);

    ///Return the number of elements in the sorted set with score between min and max.
	int zcount(const char *key, double min, double max);

    /* hash operation */

    ///Set field in the hash stored at key to value.
    int hset(const char *key, const char *field, const char *value);

    ///Sets field in the hash stored at key to value, only if field does not yet exist.
    int hsetnx(const char *key, const char *field, const char *value);

    ///Get the value associated with field in the hash stored at key.
    int hget(const char *key, const char *field, string &value);

    ///Set the specified fields to their respective values in the hash stored at key.
    int hmset(const char *key, const List &pairs);

    ///Get the values associated with the specified fields in the hash stored at key.
    int hmget(const char *key, const List &fields, List &values);

    ///Get all field names of the hash stored at key.
    int hkeys(const char *key, List &fields);

    ///Get all values of the hash stored at key.
    int hvals(const char *key, List &values);

    ///Return all fields and values of the hash stored at key.
    int hgetall(const char *key, List &fileds_values);

    ///Test if field is an existing field in the hash stored at key.
    int hexists(const char *key, const char *field);

    ///Remove field from the hash stored at key.
    int hdel(const char *key, const char *field);

    ///Return the number of fields contained in the hash stored at key.
    int hlen(const char *key);

    ///Increment the number stored at field in the hash stored at key by increment.
    int hincrby(const char *key, const char *field, int increment, int &new_val);

private:
	redic_entity_t *entity;
};

#endif //_REDIC_H_

