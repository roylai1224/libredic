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

#ifdef WIN32
#define _CRT_SECURE_NO_WARNINGS
#define _CRT_SECURE_NO_DEPRECATE
#include <winsock2.h>
#include <Ws2tcpip.h>
#define ioctl ioctlsocket
#define close closesocket
#else
#include <unistd.h>
#include <net/if.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/ioctl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <fcntl.h>
#include <sys/time.h>
#endif

typedef struct sockaddr sockaddr;
typedef struct timeval timeval;

#include <assert.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "redic.h"

bool is_in4(const char *s)
{
    for (;*s;s++)
    {
        if (isdigit(*s))
            continue;

        if (*s == '.')
            continue;

        return false;
    }

    return true;
}

bool is_in6(const char *s)
{
    for (;*s;s++)
    {
        if (isxdigit(*s))
            continue;

        if (*s == ':')
            continue;

        return false;
    }

    return true;
}

class request_t
{
private:
    string req;

public:
    request_t(int num)
    {
        char buf[16];
        sprintf(buf, "*%d\r\n", num);
        req = buf;
    }

    void append(const string &arg)
    {
        char buf[16];
        sprintf(buf, "$%d\r\n", arg.length());
        req.append(buf);
        req.append(arg);
        req.append("\r\n");
    }

    void append(const char *arg)
    {
        char buf[16];
        sprintf(buf, "$%d\r\n", strlen(arg));
        req.append(buf);
        req.append(arg);
        req.append("\r\n");
    }

    void append(int arg)
    {
        char buf[16];
        sprintf(buf, "%d", arg);
        append(buf);
    }

    void append(double arg)
    {
        char buf[16];
        sprintf(buf, "%f", arg);
        append(buf);
    }

    const char *str()
    {
        return req.c_str();
    }

    int len()
    {
        return req.length();
    }
};

#define LOG(...) printf(__VA_ARGS__),printf("\n")
#define TRC(...)

const char REDIC_ERROR	= '-';
const char REDIC_INLINE	= '+';
const char REDIC_INT	= ':';
const char REDIC_BULK	= '$';
const char REDIC_MULTI	= '*';

const int REDIC_INVALID     = 0;
const int REDIC_UNEXPECT	= -1;
const int REDIC_NOMEM 		= -2;
const int REDIC_NOFDESC		= -3;
const int REDIC_CONNFAIL	= -4;
const int REDIC_TOOBIG		= -5;
const int REDIC_TIMEOUT		= -6;
const int REDIC_PROTOERR	= -7;
const int REDIC_SVRERR      = -8;

const char *REDIC_ERRSTR[] =
{
    "invalid key value",
	"result unexpected",
	"no enough memory",
	"no file descriptor",
	"connect failure",
	"command too big",
	"operation timeout",
	"protocol error",
	"server return error",
};

class redic_entity_t
{
private:
	bool ready;

	sockaddr sa;
	timeval tv;
	int fd;

	char buffer[64];
	int head;
	int tail;

	static const int ok = 1;
	static const int xx = 0;

	int err;
    string svrerr;

public:

	redic_entity_t()
	{
#ifdef WIN32
		WSADATA data;
		WSAStartup(MAKEWORD(2,2), &data);
#endif

		ready = false;
	}

	~redic_entity_t()
	{
		disconn();
	}

	int errnum()
	{
		return err;
	}

    const char *errstr(int num)
    {
    	if (num > -1 || num < -8 )
    	{
    		return "unknown error";
    	}

        if (num == REDIC_SVRERR)
        {
            return svrerr.c_str();
        }

    	return REDIC_ERRSTR[abs(num)];
    }

	int conn(const char *host, short port)
	{
		disconn();

        if (is_in4(host))
        {
            struct in_addr in;
            inet_pton(AF_INET, host, (void *)&in);

            struct sockaddr_in *sa_in = (struct sockaddr_in *)&sa;
    		sa_in->sin_family = AF_INET;
    		sa_in->sin_port = htons(port);
    		sa_in->sin_addr = in;

    		fd = socket(AF_INET, SOCK_STREAM, 0);
    		if (fd == -1)
    		{
    			LOG("fail to open in4 socket");
    			err = REDIC_NOFDESC;
    			return xx;
    		}
        }
        else if (is_in6(host))
        {
            struct in6_addr in;
            inet_pton(AF_INET6, host, (void *)&in);

            struct sockaddr_in6 *sa_in = (struct sockaddr_in6 *)&sa;
    		sa_in->sin6_family = AF_INET6;
    		sa_in->sin6_port = htons(port);
    		sa_in->sin6_addr = in;

    		fd = socket(AF_INET6, SOCK_STREAM, 0);
    		if (fd == -1)
    		{
    			LOG("fail to open in6 socket");
    			err = REDIC_NOFDESC;
    			return xx;
    		}
        }
        else
        {
            struct hostent *he;
            he = gethostbyname(host);

            if (he == NULL)
            {
    			LOG("fail to get peer");
    			err = REDIC_CONNFAIL;
    			return xx;
            }

            if (he->h_addrtype == AF_INET)
            {
                struct sockaddr_in *sa_in = (struct sockaddr_in *)&sa;
                sa_in->sin_family = AF_INET;
                sa_in->sin_port = htons(port);
                sa_in->sin_addr = *((struct in_addr *)he->h_addr);

                fd = socket(AF_INET, SOCK_STREAM, 0);
                if (fd == -1)
                {
                	LOG("fail to open in4 socket");
                	err = REDIC_NOFDESC;
                	return xx;
                }
            }
            else if (he->h_addrtype == AF_INET6)
            {
                struct sockaddr_in6 *sa_in = (struct sockaddr_in6 *)&sa;
        		sa_in->sin6_family = AF_INET6;
        		sa_in->sin6_port = htons(port);
        		sa_in->sin6_addr = *((struct in6_addr *)he->h_addr);

        		fd = socket(AF_INET6, SOCK_STREAM, 0);
        		if (fd == -1)
        		{
        			LOG("fail to open in6 socket");
        			err = REDIC_NOFDESC;
        			return xx;
        		}
            }
            else
            {
    			LOG("fail to get peer");
    			err = REDIC_CONNFAIL;
    			return xx;
            }
        }

		if (connect(fd, &sa, sizeof(sa)))
		{
			LOG("fail to connect server");
			close(fd);
			err = REDIC_CONNFAIL;
			return xx;
		}

#ifdef WIN32
		int opt = 1;
		setsockopt(fd, SOL_SOCKET, SO_KEEPALIVE, (const char *)&opt, sizeof(opt));
		setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, (const char *)&opt, sizeof(opt));
		ioctlsocket(fd, FIONBIO, (u_long *)&opt); //nonblock
#else
		int opt = 1;
		setsockopt(fd, SOL_SOCKET, SO_KEEPALIVE, (const void *)&opt, sizeof(opt));
		setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, (const void *)&opt, sizeof(opt));

		int flg = fcntl(fd, F_GETFL);
		fcntl(fd, F_SETFL, flg | O_NONBLOCK);
#endif

        LOG("connected to server ...");
		ready = true;
		return ok;
	}

	void disconn()
	{
		if (ready)
		{
            LOG("disconnected from server ...");
			close(fd);
			ready = false;
		}
	}

	int skt_write(int fd, const char *buf, int len)
	{
        TRC("send ----------------------");
        TRC("%.*s", len, buf);
        TRC("end. ----------------------\r\n");

		assert(buf);
		assert(len > 0);

		for (int off=0; off<len;)
		{
			fd_set fds;
			FD_ZERO(&fds);
			FD_SET(fd, &fds);

			int rc = select(fd+1, NULL, &fds, NULL, &tv);
			if (rc == 0)
			{
				LOG("fail to wait for writing");
				err = REDIC_TIMEOUT;
				return 0;
			}

			if (rc < 0)
			{
				LOG("fail to select writing");
				err = REDIC_CONNFAIL;
				return 0;
			}

			rc = send(fd, buf+off, len-off, 0);
			if (rc < 0)
			{
				LOG("fail to write to server");
				err = REDIC_CONNFAIL;
				return 0;
			}

			off += rc;
		}

		return len;
	}

	int skt_read(int fd, char *buf, int len)
	{
		fd_set fds;
		FD_ZERO(&fds);
		FD_SET(fd, &fds);

		int rc = select(fd+1, &fds, NULL, NULL, &tv);
		if (rc == 0)
		{
			LOG("fail to wait for reading");
			err = REDIC_TIMEOUT;
			return 0;
		}

		if (rc < 0)
		{
			LOG("fail to select reading");
			err = REDIC_CONNFAIL;
			return 0;
		}

		rc = recv(fd, buf, len, 0);
		if (rc <= 0)
		{
			LOG("fail to read from server");
			err = REDIC_CONNFAIL;
			return 0;
		}

        TRC("recv ----------------------");
        TRC("%.*s", rc, buf);
        TRC("end. ----------------------\r\n");

		return rc;
	}

    int read_prefix(char &pre)
    {
        assert(tail >= head);

        if (head == tail)
        {
            int ret = skt_read(fd, buffer, sizeof(buffer));
			if (ret <= 0)
			{
				LOG("fail to read prefix");
				return xx;
			}

            head = 0;
			tail = ret;
        }

        pre = buffer[head];
        head += 1;
        return ok;
    }

    int read_crlf()
    {
        if (head == tail)
        {
            int ret = skt_read(fd, buffer, sizeof(buffer));
			if (ret <= 0)
			{
				LOG("fail to read char cr");
				return xx;
			}

            head = 0;
			tail = ret;
        }

        if (buffer[head] != '\r')
        {
            LOG("illegal postfix [%c]", buffer[head]);
            err = REDIC_PROTOERR;
            return xx;
        }

        head += 1;

        if (head == tail)
        {
            int ret = skt_read(fd, buffer, sizeof(buffer));
			if (ret <= 0)
			{
				LOG("fail to read char cf");
				return xx;
			}

            head = 0;
			tail = ret;
        }

        if (buffer[head] != '\n')
        {
            LOG("illegal postfix [%c]", buffer[head]);
            err = REDIC_PROTOERR;
            return xx;
        }

        head += 1;
        return ok;
    }

	int read_line(string &str)
	{
        str.clear();

		while (str.length() < LENGTH_MAX)
		{
            if (head == tail)
            {
                int ret = skt_read(fd, buffer, sizeof(buffer));
    			if (ret <= 0)
    			{
    				LOG("fail to read line");
    				return xx;
    			}

                head = 0;
    			tail = ret;
            }

			for (int i=head; i<tail; i++)
			{
				if (buffer[i] == '\r')
				{
					str.append(buffer+head, i-head);
					head = i;
					return ok;
				}
			}

            str.append(buffer+head, tail-head);
            head = tail;
		}

		LOG("fail to read total line");
		err = REDIC_TOOBIG;
		return xx;
	}

	int read_fixed(int num, string &str)
	{
        str.clear();

		while (str.length() < LENGTH_MAX)
		{
            if (head == tail)
            {
                int ret = skt_read(fd, buffer, sizeof(buffer));
    			if (ret <= 0)
    			{
    				LOG("fail to read fixed");
    				return xx;
    			}

                head = 0;
    			tail = ret;
            }

			if (num <= tail-head)
			{
				str.append(buffer+head, num);
                head += num;
                return ok;
			}

            str.append(buffer+head, tail-head);
            num -= tail-head;
            head = tail;
		}

		LOG("fail to read total fixed");
		err = REDIC_TOOBIG;
		return xx;
	}

	int read_error()
	{
		if (read_line(svrerr) != ok || read_crlf() != ok)
		{
			LOG("fail to read error");
			return xx;
		}

        LOG("server error :[%s]", svrerr.c_str());
        err = REDIC_SVRERR;
		return ok;
	}

    int send_req(request_t &req)
    {
        if (skt_write(fd, req.str(), req.len()) <= 0)
		{
			LOG("fail to send request");
			return xx;
		}

        return ok;
    }

	int recv_inline(string &val)
	{
        char pre;

		if (read_prefix(pre) != ok)
		{
			LOG("fail to read inline prefix");
			return xx;
		}

        if (pre == REDIC_ERROR)
        {
            read_error();
            return xx;
        }

        if (pre != REDIC_INLINE)
        {
			LOG("illegal inline prefix [%c]", pre);
            err = REDIC_PROTOERR;
			return xx;
        }

		if (read_line(val) != ok || read_crlf() != ok)
		{
			LOG("fail to read inline result");
			return xx;
		}

		return ok;
	}

	int recv_int(int &val)
	{
        char pre;
        string tmp;

		if (read_prefix(pre) != ok)
		{
			LOG("fail to read int prefix");
			return xx;
		}

        if (pre == REDIC_ERROR)
        {
            read_error();
            return xx;
        }

        if (pre != REDIC_INT)
        {
			LOG("illegal int prefix [%c]", pre);
            err = REDIC_PROTOERR;
			return xx;
        }

		if (read_line(tmp) != ok || read_crlf() != ok)
		{
			LOG("fail to read int result");
			return xx;
		}

		val = atoi(tmp.c_str());
		return ok;
	}

	int recv_bulk(string &val)
	{
        char pre;
        string tmp;

		if (read_prefix(pre) != ok)
		{
			LOG("fail to read bulk prefix");
			return xx;
		}

        if (pre == REDIC_ERROR)
        {
            read_error();
            return xx;
        }

        if (pre != REDIC_BULK)
        {
			LOG("illegal bulk prefix [%c]", pre);
            err = REDIC_PROTOERR;
			return xx;
        }

		if (read_line(tmp) != ok || read_crlf() != ok)
		{
			LOG("fail to read bulk size");
			return xx;
		}

        int num = atoi(tmp.c_str());
        if (num <= 0)
        {
			LOG("illegal bulk size [%s]", tmp.c_str());
			err = REDIC_PROTOERR;
			return xx;
        }

		if (read_fixed(num, val) != ok || read_crlf() != ok)
		{
			LOG("fail to read bulk result");
			err = REDIC_PROTOERR;
			return xx;
		}

		return ok;
	}

	int recv_list(std::list<string> &result)
	{
        char pre;
        string tmp;

		if (read_prefix(pre) != ok)
		{
			LOG("fail to read list prefix");
			return xx;
		}

        if (pre == REDIC_ERROR)
        {
            read_error();
            return xx;
        }

        if (pre != REDIC_MULTI)
        {
			LOG("illegal list prefix [%c]", pre);
            err = REDIC_PROTOERR;
			return xx;
        }

		if (read_line(tmp) != ok || read_crlf() != ok)
		{
			LOG("fail to read list size");
			return xx;
		}

		int num = atoi(tmp.c_str());
		if (num < 0)
		{
			LOG("illegal list size [%s]", tmp.c_str());
			err = REDIC_PROTOERR;
			return xx;
		}

        result.clear();

        for (int i=0; i<num; i++)
        {
            if (recv_bulk(tmp) != ok)
                return xx;

            result.push_back(tmp);
        }

		return ok;
	}

	int recv_set(std::set<string> &result)
	{
        char pre;
        string tmp;

		if (read_prefix(pre) != ok)
		{
			LOG("fail to read set prefix");
			return xx;
		}

        if (pre == REDIC_ERROR)
        {
            read_error();
            return xx;
        }

        if (pre != REDIC_MULTI)
        {
			LOG("illegal set prefix [%c]", pre);
            err = REDIC_PROTOERR;
			return xx;
        }

		if (read_line(tmp) != ok || read_crlf() != ok)
		{
			LOG("fail to read set size");
			return xx;
		}

		int num = atoi(tmp.c_str());
		if (num < 0)
		{
			LOG("illegal set size [%s]", tmp.c_str());
			err = REDIC_PROTOERR;
			return xx;
		}

        result.clear();

        for (int i=0; i<num; i++)
        {
            if (recv_bulk(tmp) != ok)
                return xx;

            result.insert(tmp);
        }

		return ok;
	}

	int operate_inline(string &result, request_t &req)
	{
		tv.tv_sec = TIMEOUT_VAL/1000;
		tv.tv_usec = (TIMEOUT_VAL%1000)*1000;

		head = 0;
		tail = 0;

        if (send_req(req) != ok)
			return xx;

		if (recv_inline(result) != ok)
			return xx;

		return ok;
	}

	int operate_bulk(string &result, request_t &req)
	{
		tv.tv_sec = TIMEOUT_VAL/1000;
		tv.tv_usec = (TIMEOUT_VAL%1000)*1000;

		head = 0;
		tail = 0;

        if (send_req(req) != ok)
			return xx;

		if (recv_bulk(result) != ok)
			return xx;

		return ok;
	}

	int operate_int(int &result, request_t &req)
	{
		tv.tv_sec = TIMEOUT_VAL/1000;
		tv.tv_usec = (TIMEOUT_VAL%1000)*1000;

		head = 0;
		tail = 0;

        if (send_req(req) != ok)
			return xx;

		if (recv_int(result) != ok)
			return xx;

		return ok;
	}

	int operate_list(std::list<string> &result, request_t &req)
	{
		tv.tv_sec = TIMEOUT_VAL/1000;
		tv.tv_usec = (TIMEOUT_VAL%1000)*1000;

		head = 0;
		tail = 0;

        if (send_req(req) != ok)
			return xx;

		if (recv_list(result) != ok)
			return xx;

		return ok;
	}

	int operate_set(std::set<string> &result, request_t &req)
	{
		tv.tv_sec = TIMEOUT_VAL/1000;
		tv.tv_usec = (TIMEOUT_VAL%1000)*1000;

		head = 0;
		tail = 0;

        if (send_req(req) != ok)
			return xx;

		if (recv_set(result) != ok)
			return xx;

		return ok;
	}
};


redic_t::redic_t()
{
	entity = NULL;
}

redic_t::~redic_t()
{
	if (entity)
	{
		delete entity;
	}
}

const char *redic_t::errstr(int num)
{
    if (entity)
    {
        return entity->errstr(num);
    }
    else
    {
        return "not connected yet";
    }
}

int redic_t::connect(const char *host, short port)
{
    host = host ? host : "localhost";
    port = port ? port : 6379;

	if (!entity)
	{
		entity = new redic_entity_t;
		if (!entity)
		{
			LOG("fail to alloc entity");
			return REDIC_NOMEM;
		}
	}

	return entity->conn(host, port);
}

void redic_t::disconn()
{
	if (entity)
	{
		entity->disconn();
	}
}

int redic_t::auth(const char *password)
{
    request_t req(2);
    req.append("AUTH");
    req.append(password);

	string result;

	if (entity->operate_inline(result, req) != ok)
		return entity->errnum();

	if (result != "OK")
		return REDIC_UNEXPECT;

	return ok;
}

int redic_t::info(string &info)
{
    request_t req(1);
    req.append("INFO");

	if (entity->operate_bulk(info, req) != ok)
		return entity->errnum();

	return ok;
}

int redic_t::ping()
{
    request_t req(1);
    req.append("PING");

	string result;

	if (entity->operate_inline(result, req) != ok)
		return entity->errnum();

	if (result != "PONG")
		return REDIC_UNEXPECT;

	return ok;
}

int redic_t::save()
{
    request_t req(1);
    req.append("SAVE");

	string result;

	if (entity->operate_inline(result, req) != ok)
		return entity->errnum();

	if (result != "OK")
		return REDIC_UNEXPECT;

	return ok;
}

int redic_t::bgsave()
{
    request_t req(1);
    req.append("BGSAVE");

	string result;

	if (entity->operate_inline(result, req) != ok)
		return entity->errnum();

	//result is some tips string
	return ok;
}

int redic_t::lastsave(time_t &tm)
{
    request_t req(1);
    req.append("LASTSAVE");

	int result;

	if (entity->operate_int(result, req) != ok)
		return entity->errnum();

	tm = result;
	return ok;
}

int redic_t::bgrewriteaof()
{
    request_t req(1);
    req.append("BGREWRITEAOF");

	string result;

	if (entity->operate_inline(result, req) != ok)
		return entity->errnum();

	if (result != "OK")
		return REDIC_UNEXPECT;

	return ok;
}

int redic_t::select(int index)
{
    request_t req(2);
    req.append("SELECT");
    req.append(index);

	string result;

	if (entity->operate_inline(result, req) != ok)
		return entity->errnum();

	if (result != "OK")
		return REDIC_UNEXPECT;

	return ok;
}

int redic_t::randomkey(string &key)
{
    request_t req(1);
    req.append("RANDOMKEY");

	if (entity->operate_bulk(key, req) != ok)
		return entity->errnum();

	return ok;
}

int redic_t::dbsize()
{
    request_t req(1);
    req.append("DBSIZE");

    int result;

	if (entity->operate_int(result, req) != ok)
		return entity->errnum();

    if (result < 0)
        return REDIC_UNEXPECT;

	return result;
}

int redic_t::flushdb()
{
    request_t req(1);
    req.append("FLUSHDB");

	string result;

	if (entity->operate_inline(result, req) != ok)
		return entity->errnum();

	if (result != "OK")
		return REDIC_UNEXPECT;

	return ok;
}

int redic_t::flushall()
{
    request_t req(1);
    req.append("FLUSHALL");

	string result;

	if (entity->operate_inline(result, req) != ok)
		return entity->errnum();

	if (result != "OK")
		return REDIC_UNEXPECT;

	return ok;
}

int redic_t::keys(const char *pattern, List &keys)
{
    request_t req(2);
    req.append("KEYS");
    req.append(pattern);

	if (entity->operate_list(keys, req) != ok)
		return entity->errnum();

	return ok;
}

int redic_t::exists(const char *key)
{
    request_t req(2);
    req.append("EXISTS");
    req.append(key);

	int result;

	if (entity->operate_int(result, req) != ok)
		return entity->errnum();

    if (result == 0)
        return REDIC_INVALID;
        
    if (result != 1)
    	return REDIC_UNEXPECT;

	return ok;
}

int redic_t::del(const char *key)
{
    request_t req(2);
    req.append("DEL");
    req.append(key);

	int result;

	if (entity->operate_int(result, req) != ok)
		return entity->errnum();

    if (result == 0)
        return REDIC_INVALID;

	if (result != 1)
		return REDIC_UNEXPECT;

	return ok;
}

int redic_t::type(const char *key, string &type)
{
    request_t req(2);
    req.append("TYPE");
    req.append(key);

	if (entity->operate_inline(type, req) != ok)
		return entity->errnum();

    if (type == "none")
        return REDIC_INVALID;

	return ok;
}

int redic_t::rename(const char *key, const char *newkey)
{
    request_t req(3);
    req.append("RENAME");
    req.append(key);
    req.append(newkey);

	string result;

	if (entity->operate_inline(result, req) != ok)
		return entity->errnum();

	if (result != "OK")
		return REDIC_UNEXPECT;

	return ok;
}

int redic_t::renamenx(const char *key, const char *newkey)
{
    request_t req(3);
    req.append("RENAMENX");
    req.append(key);
    req.append(newkey);

	int result;

	if (entity->operate_int(result, req) != ok)
		return entity->errnum();

    if (result == 0)
        return REDIC_INVALID;

	if (result != 1)
		return REDIC_UNEXPECT;

	return ok;
}

int redic_t::expire(const char *key, int secs)
{
    request_t req(3);
    req.append("EXPIRE");
    req.append(key);
    req.append(secs);

	int result;

	if (entity->operate_int(result, req) != ok)
		return entity->errnum();

	if (result == 0)
		return REDIC_INVALID;

    if (result != 1)
        return REDIC_UNEXPECT;

	return ok;
}

int redic_t::ttl(const char *key)
{
    request_t req(2);
    req.append("TTL");
    req.append(key);

	int result;

	if (entity->operate_int(result, req) != ok)
		return entity->errnum();

    if (result < 0)
        return REDIC_UNEXPECT;

	return result;
}

int redic_t::move(const char *key, int index)
{
    request_t req(3);
    req.append("MOVE");
    req.append(key);
    req.append(index);

	int result;

	if (entity->operate_int(result, req) != ok)
		return entity->errnum();

	if (result == 0)
		return REDIC_INVALID;

    if (result != 1)
        return REDIC_UNEXPECT;

	return ok;
}

int redic_t::append(const char *key, const char *value)
{
    request_t req(3);
    req.append("APPEND");
    req.append(key);
    req.append(value);

	int result;

	if (entity->operate_int(result, req) != ok)
		return entity->errnum();

    if (result < 0)
        return REDIC_UNEXPECT;

	return result;
}

int redic_t::set(const char *key, const char *value)
{
    request_t req(3);
    req.append("SET");
    req.append(key);
    req.append(value);

	string result;

	if (entity->operate_inline(result, req) != ok)
		return entity->errnum();

	if (result != "OK")
		return REDIC_UNEXPECT;

	return ok;
}

int redic_t::get(const char *key, string &value)
{
    request_t req(2);
    req.append("GET");
    req.append(key);

	if (entity->operate_bulk(value, req) != ok)
		return entity->errnum();

	return ok;
}

int redic_t::getset(const char *key, const char *value, string &old_val)
{
    request_t req(3);
    req.append("GETSET");
    req.append(key);
    req.append(value);

	if (entity->operate_bulk(old_val, req) != ok)
		return entity->errnum();

    return ok;
}

int redic_t::setex(const char *key, int secs, const char *value)
{
    request_t req(4);
    req.append("SETEX");
    req.append(key);
    req.append(secs);
    req.append(value);

    string result;

	if (entity->operate_inline(result, req) != ok)
		return entity->errnum();

    if (result != "OK")
        return REDIC_UNEXPECT;

    return ok;
}

int redic_t::setnx(const char *key, const char *value)
{
    request_t req(3);
    req.append("SETNX");
    req.append(key);
    req.append(value);

    int result;

	if (entity->operate_int(result, req) != ok)
		return entity->errnum();

    if (result == 0)
        return REDIC_INVALID;

    if (result != 1)
        return REDIC_UNEXPECT;

    return ok;
}

int redic_t::strlen(const char *key)
{
    request_t req(2);
    req.append("STRLEN");
    req.append(key);

    int result;

	if (entity->operate_int(result, req) != ok)
		return entity->errnum();

    if (result < 0)
        return REDIC_UNEXPECT;

	return result;
}

int redic_t::substr(const char *key, int start, int end, string &value)
{
    request_t req(4);
    req.append("SUBSTR");
    req.append(key);
    req.append(start);
    req.append(end);

	if (entity->operate_bulk(value, req) != ok)
		return entity->errnum();

    return ok;
}

int redic_t::mget(const List &keys, List &values)
{
    request_t req(1+keys.size());
    req.append("MGET");

	for(List::const_iterator it=keys.begin(); it!=keys.end(); it++)
        req.append(*it);

	if (entity->operate_list(values, req) != ok)
		return entity->errnum();

    return ok;
}

int redic_t::incr(const char *key, int &new_val)
{
    request_t req(2);
    req.append("INCR");
    req.append(key);

	if (entity->operate_int(new_val, req) != ok)
		return entity->errnum();

    return ok;
}

int redic_t::incrby(const char *key, int increment, int &new_val)
{
    request_t req(3);
    req.append("INCRBY");
    req.append(key);
    req.append(increment);

	if (entity->operate_int(new_val, req) != ok)
		return entity->errnum();

    return ok;
}

int redic_t::decr(const char *key, int &new_val)
{
    request_t req(2);
    req.append("DECR");
    req.append(key);

	if (entity->operate_int(new_val, req) != ok)
		return entity->errnum();

    return ok;
}

int redic_t::decrby(const char *key, int decrement, int &new_val)
{
    request_t req(3);
    req.append("DECRBY");
    req.append(key);
    req.append(decrement);

	if (entity->operate_int(new_val, req) != ok)
		return entity->errnum();

    return ok;
}

int redic_t::rpush(const char *key, const char *element)
{
    request_t req(3);
    req.append("RPUSH");
    req.append(key);
    req.append(element);

    int result;

	if (entity->operate_int(result, req) != ok)
		return entity->errnum();

    if (result < 0)
        return REDIC_UNEXPECT;

	return result;
}

int redic_t::rpushx(const char *key, const char *element)
{
    request_t req(3);
    req.append("RPUSHX");
    req.append(key);
    req.append(element);

    int result;

	if (entity->operate_int(result, req) != ok)
		return entity->errnum();

    if (result < 0)
        return REDIC_UNEXPECT;

	return result;
}

int redic_t::lpush(const char *key, const char *element)
{
    request_t req(3);
    req.append("LPUSH");
    req.append(key);
    req.append(element);

    int result;

	if (entity->operate_int(result, req) != ok)
		return entity->errnum();

    if (result < 0)
        return REDIC_UNEXPECT;

	return result;
}

int redic_t::lpushx(const char *key, const char *element)
{
    request_t req(3);
    req.append("LPUSHX");
    req.append(key);
    req.append(element);

    int result;

	if (entity->operate_int(result, req) != ok)
		return entity->errnum();

    if (result < 0)
        return REDIC_UNEXPECT;

	return result;
}

int redic_t::lpop(const char *key, string &element)
{
    request_t req(2);
    req.append("LPOP");
    req.append(key);

	if (entity->operate_bulk(element, req) != ok)
		return entity->errnum();

	return ok;
}

int redic_t::rpop(const char *key, string &element)
{
    request_t req(2);
    req.append("RPOP");
    req.append(key);

	if (entity->operate_bulk(element, req) != ok)
		return entity->errnum();

	return ok;
}

int redic_t::llen(const char *key)
{
    request_t req(2);
    req.append("LLEN");
    req.append(key);

    int result;

	if (entity->operate_int(result, req) != ok)
		return entity->errnum();

    if (result < 0)
        return REDIC_UNEXPECT;

	return result;
}

int redic_t::lrange(const char *key, int start, int range, List &elements)
{
    request_t req(4);
    req.append("LRANGE");
    req.append(key);
    req.append(start);
    req.append(range);

	if (entity->operate_list(elements, req) != ok)
		return entity->errnum();

	return ok;
}

int redic_t::ltrim(const char *key, int start, int end)
{
    request_t req(4);
    req.append("LTRIM");
    req.append(key);
    req.append(start);
    req.append(end);

    string result;

	if (entity->operate_inline(result, req) != ok)
		return entity->errnum();

    if (result != "OK")
        return REDIC_UNEXPECT;

	return ok;
}

int redic_t::lset(const char *key, int index, const char *element)
{
    request_t req(4);
    req.append("LSET");
    req.append(key);
    req.append(index);
    req.append(element);

    string result;

	if (entity->operate_inline(result, req) != ok)
		return entity->errnum();

    if (result != "OK")
        return REDIC_UNEXPECT;

	return ok;
}

int redic_t::lindex(const char *key, int index, string &element)
{
    request_t req(3);
    req.append("LINDEX");
    req.append(key);
    req.append(index);

	if (entity->operate_bulk(element, req) != ok)
		return entity->errnum();

	return ok;
}

///count > 0: Remove elements from head to tail.
///count < 0: Remove elements from tail to head.
///count = 0: Remove all elements.
int redic_t::lrem(const char *key, int count, const char *element)
{
    request_t req(4);
    req.append("LREM");
    req.append(key);
    req.append(count);
    req.append(element);

    int result;

	if (entity->operate_int(result, req) != ok)
		return entity->errnum();

    if (result < 0)
        return REDIC_UNEXPECT;

	return result;
}

int redic_t::sadd(const char *key, const char *member)
{
    request_t req(3);
    req.append("SADD");
    req.append(key);
    req.append(member);

    int result;

	if (entity->operate_int(result, req) != ok)
		return entity->errnum();

    if (result == 0)
        return REDIC_INVALID;

    if (result != 1)
        return REDIC_UNEXPECT;

	return ok;
}

int redic_t::srem(const char *key, const char *member)
{
    request_t req(3);
    req.append("SREM");
    req.append(key);
    req.append(member);

    int result;

	if (entity->operate_int(result, req) != ok)
		return entity->errnum();

    if (result == 0)
        return REDIC_INVALID;

    if (result != 1)
        return REDIC_UNEXPECT;

	return ok;
}

int redic_t::spop(const char *key, string &value)
{
    request_t req(2);
    req.append("SPOP");
    req.append(key);

	if (entity->operate_bulk(value, req) != ok)
		return entity->errnum();

	return ok;
}

int redic_t::smove(const char *srckey, const char *destkey, const char *member)
{
    request_t req(4);
    req.append("SMOVE");
    req.append(srckey);
    req.append(destkey);
    req.append(member);

    int result;

	if (entity->operate_int(result, req) != ok)
		return entity->errnum();

    if (result == 0)
        return REDIC_INVALID;

    if (result != 1)
        return REDIC_UNEXPECT;

	return ok;
}

int redic_t::scard(const char *key)
{
    request_t req(2);
    req.append("SCARD");
    req.append(key);

    int result;

	if (entity->operate_int(result, req) != ok)
		return entity->errnum();

    if (result < 0)
        return REDIC_UNEXPECT;

	return result;
}

int redic_t::sismember(const char *key, const char *member)
{
    request_t req(3);
    req.append("SISMEMBER");
    req.append(key);
    req.append(member);

    int result;

	if (entity->operate_int(result, req) != ok)
		return entity->errnum();

    if (result == 0)
        return REDIC_INVALID;

    if (result != 1)
        return REDIC_UNEXPECT;

	return result;
}

int redic_t::sinter(const Set &keys, Set &members)
{
    request_t req(1+keys.size());
    req.append("SINTER");

	for(Set::const_iterator it=keys.begin(); it!=keys.end(); it++)
        req.append(*it);

	if (entity->operate_set(members, req) != ok)
		return entity->errnum();

    return ok;
}

int redic_t::sinterstore(const char *destkey, const Set &keys)
{
    request_t req(2+keys.size());
    req.append("SINTERSTORE");
    req.append(destkey);

	for(Set::const_iterator it=keys.begin(); it!=keys.end(); it++)
        req.append(*it);

    int result;

	if (entity->operate_int(result, req) != ok)
		return entity->errnum();

    if (result < 0)
        return REDIC_UNEXPECT;

    return result;
}

int redic_t::sunion(const Set &keys, Set &members)
{
    request_t req(1+keys.size());
    req.append("SUNION");

	for(Set::const_iterator it=keys.begin(); it!=keys.end(); it++)
        req.append(*it);

	if (entity->operate_set(members, req) != ok)
		return entity->errnum();

    return ok;
}

int redic_t::sunionstore(const char *destkey, const Set &keys)
{
    request_t req(2+keys.size());
    req.append("SUNIONSTORE");
    req.append(destkey);

	for(Set::const_iterator it=keys.begin(); it!=keys.end(); it++)
        req.append(*it);

    int result;

	if (entity->operate_int(result, req) != ok)
		return entity->errnum();

    if (result < 0)
        return REDIC_UNEXPECT;

    return result;
}

int redic_t::sdiff(const Set &keys, Set &members)
{
    request_t req(1+keys.size());
    req.append("SDIFF");

	for(Set::const_iterator it=keys.begin(); it!=keys.end(); it++)
        req.append(*it);

	if (entity->operate_set(members, req) != ok)
		return entity->errnum();

    return ok;
}

int redic_t::sdiffstore(const char *destkey, const Set &keys)
{
    request_t req(2+keys.size());
    req.append("SDIFFSTORE");
    req.append(destkey);

	for(Set::const_iterator it=keys.begin(); it!=keys.end(); it++)
        req.append(*it);

    int result;

	if (entity->operate_int(result, req) != ok)
		return entity->errnum();

    if (result < 0)
        return REDIC_UNEXPECT;

    return result;
}

int redic_t::smembers(const char *key, Set &members)
{
    request_t req(2);
    req.append("SMEMBERS");
    req.append(key);

	if (entity->operate_set(members, req) != ok)
		return entity->errnum();

	return ok;
}

int redic_t::srandmember(const char *key, string &member)
{
    request_t req(2);
    req.append("SRANDMEMBER");
    req.append(key);

	if (entity->operate_bulk(member, req) != ok)
		return entity->errnum();

	return ok;
}

int redic_t::zadd(const char *key, double score, const char *member)
{
    request_t req(4);
    req.append("ZADD");
    req.append(key);
    req.append(score);
    req.append(member);

    int result;

	if (entity->operate_int(result, req) != ok)
		return entity->errnum();

    if (result != 0 && result != 1)
        return REDIC_UNEXPECT;

	return ok;
}

int redic_t::zrem(const char *key, const char *member)
{
    request_t req(3);
    req.append("ZREM");
    req.append(key);
    req.append(member);

    int result;

	if (entity->operate_int(result, req) != ok)
		return entity->errnum();

    if (result == 0)
        return REDIC_INVALID;

    if (result != 1)
        return REDIC_UNEXPECT;

	return ok;
}

int redic_t::zincrby(const char *key, double increment, const char *member, double &new_score)
{
    request_t req(4);
    req.append("ZINCRBY");
    req.append(key);
    req.append(increment);
    req.append(member);

    string result;

	if (entity->operate_bulk(result, req) != ok)
		return entity->errnum();

    new_score = atof(result.c_str());
	return ok;
}

int redic_t::zrank(const char *key, const char *member)
{
    request_t req(3);
    req.append("ZRANK");
    req.append(key);
    req.append(member);

    int result;

	if (entity->operate_int(result, req) != ok)
		return entity->errnum();

    if (result < 0)
        return REDIC_UNEXPECT;

	return result;
}

int redic_t::zrevrank(const char *key, const char *member)
{
    request_t req(3);
    req.append("ZREVRANK");
    req.append(key);
    req.append(member);

    int result;

	if (entity->operate_int(result, req) != ok)
		return entity->errnum();

    if (result < 0)
        return REDIC_UNEXPECT;

	return result;
}

int redic_t::zrange(const char *key, int start, int stop, List &elements)
{
    request_t req(4);
    req.append("ZRANGE");
    req.append(key);
    req.append(start);
    req.append(stop);

	if (entity->operate_list(elements, req) != ok)
		return entity->errnum();

	return ok;
}

int redic_t::zrevrange(const char *key, int start, int stop, List &elements)
{
    request_t req(4);
    req.append("ZREVRANGE");
    req.append(key);
    req.append(start);
    req.append(stop);

	if (entity->operate_list(elements, req) != ok)
		return entity->errnum();

	return ok;
}

int redic_t::zcard(const char *key)
{
    request_t req(2);
    req.append("ZCARD");
    req.append(key);

    int result;

	if (entity->operate_int(result, req) != ok)
		return entity->errnum();

    if (result == 0)
        return REDIC_INVALID;

    if (result < 0)
        return REDIC_UNEXPECT;

	return result;
}

int redic_t::zscore(const char *key, const char *member, double &score)
{
    request_t req(3);
    req.append("ZSCORE");
    req.append(key);
    req.append(member);

    string result;

	if (entity->operate_bulk(result, req) != ok)
		return entity->errnum();

    score = atof(result.c_str());
	return ok;
}

int redic_t::zremrangebyscore(const char *key, double min, double max)
{
    request_t req(4);
    req.append("ZREMRANGEBYSCORE");
    req.append(key);
    req.append(min);
    req.append(max);

    int result;

	if (entity->operate_int(result, req) != ok)
		return entity->errnum();

    if (result < 0)
        return REDIC_UNEXPECT;

	return result;
}

int redic_t::zremrangebyrank(const char *key, int start, int stop)
{
    request_t req(4);
    req.append("ZREMRANGEBYRANK");
    req.append(key);
    req.append(start);
    req.append(stop);

    int result;

	if (entity->operate_int(result, req) != ok)
		return entity->errnum();

    if (result < 0)
        return REDIC_UNEXPECT;

	return result;
}

int redic_t::zcount(const char *key, double min, double max)
{
    request_t req(4);
    req.append("ZCOUNT");
    req.append(key);
    req.append(min);
    req.append(max);

    int result;

	if (entity->operate_int(result, req) != ok)
		return entity->errnum();

    if (result < 0)
        return REDIC_UNEXPECT;

	return result;
}

int redic_t::hset(const char *key, const char *field, const char *value)
{
    request_t req(4);
    req.append("HSET");
    req.append(key);
    req.append(field);
    req.append(value);

    int result;

	if (entity->operate_int(result, req) != ok)
		return entity->errnum();

    if (result != 0 && result != 1)
        return REDIC_UNEXPECT;

	return ok;
}

int redic_t::hsetnx(const char *key, const char *field, const char *value)
{
    request_t req(4);
    req.append("HSETNX");
    req.append(key);
    req.append(field);
    req.append(value);

    int result;

	if (entity->operate_int(result, req) != ok)
		return entity->errnum();

    if (result == 0)
        return REDIC_INVALID;

    if (result != 1)
        return REDIC_UNEXPECT;

	return ok;
}

int redic_t::hget(const char *key, const char *field, string &value)
{
    request_t req(3);
    req.append("HGET");
    req.append(key);
    req.append(field);

	if (entity->operate_bulk(value, req) != ok)
		return entity->errnum();

	return ok;
}

int redic_t::hmset(const char *key, const List &pairs)
{
    request_t req(2+pairs.size());
    req.append("HMSET");
    req.append(key);

	for(List::const_iterator it=pairs.begin(); it!=pairs.end(); it++)
        req.append(*it);

    string result;

	if (entity->operate_inline(result, req) != ok)
		return entity->errnum();

    if (result != "OK")
        return REDIC_UNEXPECT;

	return ok;
}

int redic_t::hmget(const char *key, const List &fields, List &values)
{
    request_t req(2+fields.size());
    req.append("HMGET");
    req.append(key);

	for(List::const_iterator it=fields.begin(); it!=fields.end(); it++)
        req.append(*it);

	if (entity->operate_list(values, req) != ok)
		return entity->errnum();

	return ok;
}

int redic_t::hkeys(const char *key, List &fields)
{
    request_t req(2);
    req.append("HKEYS");
    req.append(key);

	if (entity->operate_list(fields, req) != ok)
		return entity->errnum();

	return ok;
}

int redic_t::hvals(const char *key, List &values)
{
    request_t req(2);
    req.append("HVALS");
    req.append(key);

	if (entity->operate_list(values, req) != ok)
		return entity->errnum();

	return ok;
}

int redic_t::hgetall(const char *key, List &pairs)
{
    request_t req(2);
    req.append("HGETALL");
    req.append(key);

	if (entity->operate_list(pairs, req) != ok)
		return entity->errnum();

	return ok;
}

int redic_t::hexists(const char *key, const char *field)
{
    request_t req(3);
    req.append("HEXISTS");
    req.append(key);
    req.append(field);

    int result;

	if (entity->operate_int(result, req) != ok)
		return entity->errnum();

    if (result == 0)
        return REDIC_INVALID;

    if (result != 1)
        return REDIC_UNEXPECT;

	return ok;
}

int redic_t::hdel(const char *key, const char *field)
{
    request_t req(3);
    req.append("HDEL");
    req.append(key);
    req.append(field);

    int result;

	if (entity->operate_int(result, req) != ok)
		return entity->errnum();

    if (result == 0)
        return REDIC_INVALID;

    if (result != 1)
        return REDIC_UNEXPECT;

	return ok;
}

int redic_t::hlen(const char *key)
{
    request_t req(2);
    req.append("HLEN");
    req.append(key);

    int result;

	if (entity->operate_int(result, req) != ok)
		return entity->errnum();

    if (result < 0)
        return REDIC_UNEXPECT;

	return result;
}

int redic_t::hincrby(const char *key, const char *field, int increment, int &new_val)
{
    request_t req(4);
    req.append("HINCRBY");
    req.append(key);
    req.append(field);
    req.append(increment);

	if (entity->operate_int(new_val, req) != ok)
		return entity->errnum();

	return ok;
}
