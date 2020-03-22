
Requires https://github.com/deepfryed/beanstalk-client


Notes for FreeBSD:

	service beanstalkd looks strange (unchecked)
	I had to:
		sudo /usr/local/bin/beanstalkd -l 127.0.0.1 -p 11300

