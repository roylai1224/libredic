CC = g++
CCFLAGS = -g -Wall
LDFLAGS =


libredic.a: redic.o
	$(AR) -cvq $@ $^

test: redic.o test.o
	$(CC) $(CCFLAGS) $(LDFLAGS) -lpthread -lgtest -o $@ $^

redic.o: redic.cc redic.h Makefile
	$(CC) $(CCFLAGS) -c -fPIC -o $@ redic.cc

test.o: test.cc
	$(CC) $(CCFLAGS) -c -fPIC -o $@ test.cc

clean:
	rm -f *.o *~ test libredic.a
