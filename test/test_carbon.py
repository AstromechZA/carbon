import unittest
import sys
import time
import os
import platform
import subprocess
import os.path
import shutil
import random
import tempfile
from socket import socket
from ConfigParser import ConfigParser

# Check dependencies
try:
    import whisper
except Exception as e:
    print('Missing required dependency: Whisper=0.9.10')
    exit(1)
try:
    import twisted
except Exception as e:
    print('Missing required dependency: Twisted=11.10.1')
    exit(1)

CARBON_SERVER = '127.0.0.1'
CARBON_PORT = 2003

class CarbonTestCase(unittest.TestCase):

    # Paths
    test_dir = os.path.dirname(os.path.abspath(__file__))                       # this is the test dir
    carbon_dir = os.path.dirname(test_dir)                                     # path to carbon dir
    temp_dir = tempfile.mkdtemp()                                               # some temporary dir

    test_conf = os.path.join(test_dir, 'conf', 'carbon.conf')                # path to test config
    test_stor = os.path.join(test_dir, 'conf', 'storage-schemas.conf')       # path to test config

    print('temp dir: %s' % temp_dir)
    print('test dir: %s' % test_dir)

    step = 0
    max_datapoints = 0
    MAX_SAMPLE = 20

    carbonp = None

    @classmethod
    def setUpClass(cls):
        os.putenv("GRAPHITE_ROOT", cls.temp_dir)                                # this is where temporary files and storage will end up

        carboncachepath = os.path.join(cls.carbon_dir, 'bin', 'carbon-cache.py')

        cls.carbonp = subprocess.Popen(["python", carboncachepath, "--config=" + cls.test_conf, "start"])

        # Extract test retentions from 'storage-schemas.conf'
        # Here we have assumed that 'storage-schemas.conf' only has one section;
        # this section must have a 'retention' option.
        config_parser = ConfigParser()
        if not config_parser.read(cls.test_stor):
            print "Error: Couldn't read config file: %s" % cls.test_stor

        retentions = ""

        section = config_parser.sections()[0]
        print "Section '%s':" % section
        options = dict(config_parser.items(config_parser.sections()[0]))
        retentions = whisper.parseRetentionDef(options['retentions'])

        cls.step = retentions[0]
        cls.max_datapoints = retentions[1]

        time.sleep(2)                                                   # NB - allows file operations to complete

    def runTest(self):
        tags = ['abc', 'def']

        sock = socket()
        try:
            sock.connect( (CARBON_SERVER,CARBON_PORT) )
        except Exception as e:
            self.fail("could not connect")

        # Create some sample data
        num_data_points = min(self.MAX_SAMPLE, self.max_datapoints)
        now = int(time.time())
        now -= now % self.step
        data = []
        lines = []
        for i in range(1, num_data_points+1):
            data.append((now - self.step*(num_data_points - i), random.random()*100))
            for tag in tags:
                lines.append("folder.%s %s %d" % (tag, data[-1][1], data[-1][0]))

        message = '\n'.join(lines) + '\n' #all lines must end in a newline

        # debug
        print "sending message"
        print '-' * 70
        print message

        # send!
        sock.sendall(message)
        time.sleep(2) # NB - allows file operations to complete

        print('data starts at: ' + str(data[0][0]))
        print('current time is:' + str(time.time()))
        print len(data)
        # check if data files were created

        for i,tag in enumerate(tags):
            tagFile = os.path.join(self.temp_dir, "storage","whisper","folder", tag + ".wsp")
            self.assertTrue(os.path.exists(tagFile))

            # check if data files contain correct data
            # print(whisper.info(tagFile))
            print 'from, until:', (now - self.step*(num_data_points), now)
            # print(whisper.fetch(tagFile, now - self.step*(num_data_points), now))
            # The values passed to whisper.fetch for 'from' and 'to' are
            # rounded up to the next discrete time point. As a result, we subtract one step from them
            # Additionally, the upper limit is non-inclusive in the fetch
            # and so we need to increment our 'to' value by a step-size
            data_period_info, stored_data = whisper.fetch(tagFile, now - self.step*(num_data_points), now)
            # print len(stored_data)

            # Check that all fetched data corresponds to the data that was sent
            # (Note: some of the sent data may have rolled off the retained data
            # so only the fetched data that is still retained is compared)
            for whisper_data, sent_data in zip(reversed(stored_data), reversed(data)):
                self.assertAlmostEquals(whisper_data, sent_data[1])

    @classmethod
    def tearDownClass(cls):


        if os.name == 'nt':
            pidpath = os.path.join(cls.temp_dir, "storage")
            if not os.path.exists(pidpath):
                os.makedirs(pidpath)
            pidfilepath = os.path.join(pidpath, 'carbon-cache-a.pid')
            print('Creating "'+pidfilepath+'" for ' + str(cls.carbonp.pid))
            pidf = open(pidfilepath, 'w')
            pidf.write(str(cls.carbonp.pid))
            pidf.close()

        carboncachepath = os.path.join(cls.carbon_dir, 'bin', 'carbon-cache.py')
        p = subprocess.Popen(["python", carboncachepath, "--config=" + cls.test_conf, "stop"])
        while p.poll() == None:
            time.sleep(0.1)
        shutil.rmtree(cls.temp_dir)

if __name__ == '__main__':
    unittest.main()


