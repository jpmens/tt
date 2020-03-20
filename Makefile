CFLAGS=-I/Users/jpm/syncthing/tiggr/libs/beanstalk-client/
LDFLAGS=-L /Users/jpm/syncthing/tiggr/libs/beanstalk-client/ -l beanstalk

## FreeBSD
#
#	obtain beanstalk.[ch] from https://github.com/deepfryed/beanstalk-client
#	and `cc -c beanstalk.c'
#
# CFLAGS=-Ibeanstalk-client/ -I/usr/local/include
# LDFLAGS=-L /usr/local/lib beanstalk-client/beanstalk.o

all: tt

tt: tt.c
	$(CC) -Wall -Werror $(CFLAGS) -o tt tt.c -lcurl $(LDFLAGS)
