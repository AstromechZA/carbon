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
CARBON_PORT = 2023



class CarbonTestCase(unittest.TestCase):

    # Paths
    test_dir = os.path.dirname(os.path.abspath(__file__))                       # this is the test dir
    carbon_dir = os.path.dirname(test_dir)                                     # path to carbon dir
    temp_dir = tempfile.mkdtemp()                                               # some temporary dir

    test_conf = os.path.join(test_dir, 'conf', 'carbon.conf')                # path to test config
    test_stor = os.path.join(test_dir, 'conf', 'storage-schemas.conf')       # path to test config

    carboncachepath = os.path.join(carbon_dir, 'bin', 'carbon-cache.py')
    carbonaggrpath = os.path.join(carbon_dir, 'bin', 'carbon-aggregator.py')

    print('temp dir: %s' % temp_dir)
    print('test dir: %s' % test_dir)

    step = 0
    max_datapoints = 0
    MAX_SAMPLE = 20

    carboncachep = None
    carbonaggrp = None

    @classmethod
    def setUpClass(cls):
        os.putenv("GRAPHITE_ROOT", cls.temp_dir)                                # this is where temporary files and storage will end up

        cls.carboncachep = subprocess.Popen(["python", cls.carboncachepath, "--config=" + cls.test_conf, "start"])
        cls.carbonaggrp = subprocess.Popen(["python", cls.carbonaggrpath, "--config=" + cls.test_conf, "start"])

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

        print('step size: %d' % cls.step)

        time.sleep(2)                                                   # NB - allows file operations to complete

    def runTest(self):
        tag = 'random_data_cca'

        sock = socket()
        try:
            sock.connect( (CARBON_SERVER,CARBON_PORT) )
        except Exception as e:
            self.fail("could not connect")

        # Create some sample data
        num_data_points = 5
        num_substep = 10

        data = []
        lines = []

        start = (time.time())
        start = start - (start % self.step)
        last = start


        stime = float(float(self.step)/float(num_substep))

        pts = (num_data_points)*(num_substep)
        tp = 0.0

        print('Bin is ' + str(self.step) + ' seconds.')
        print('Adding ' + str(1.0/stime) + ' points a second for ' + str(num_data_points*self.step) + ' seconds.')

        print('0.0%')
        for i in range(num_data_points):

            to_aggregate = []

            for tick in range(num_substep):

                to_aggregate.append(  (last, random.random()*100)  )

                line = "folder.%s %s %d \n" % (tag, to_aggregate[-1][1], to_aggregate[-1][0])
                sock.sendall(line)

                tp+=1.0

                print(str((tp/pts)*100) + '%')

                last += stime
                time.sleep(stime)


            aggregated_data = aggregate(to_aggregate)
            data.append(  aggregated_data  )

        print('')

        time.sleep(2) # NB - allows file operations to complete

        tagFile = os.path.join(self.temp_dir, "storage","whisper","folder", tag + ".wsp")

        self.assertTrue(os.path.exists(tagFile))

        data_period_info, stored_data = whisper.fetch(tagFile, start-1, last)
        print len(stored_data)

        for whisper_data, sent_data in zip(stored_data, data):
            self.assertAlmostEquals(whisper_data, sent_data)

    @classmethod
    def tearDownClass(cls):

        if os.name == 'nt':
            pidpath = os.path.join(cls.temp_dir, "storage")
            if not os.path.exists(pidpath):
                os.makedirs(pidpath)
            ccpidfilepath = os.path.join(pidpath, 'carbon-cache-a.pid')
            capidfilepath = os.path.join(pidpath, 'carbon-aggregator-a.pid')

            print('Creating "'+ccpidfilepath+'" for ' + str(cls.carboncachep.pid))
            pidf = open(ccpidfilepath, 'w')
            pidf.write(str(cls.carboncachep.pid))
            pidf.close()

            print('Creating "'+capidfilepath+'" for ' + str(cls.carbonaggrp.pid))
            pidf = open(capidfilepath, 'w')
            pidf.write(str(cls.carbonaggrp.pid))
            pidf.close()

        subprocess.Popen(["python", cls.carbonaggrpath, "--config=" + cls.test_conf, "stop"])
        p = subprocess.Popen(["python", cls.carboncachepath, "--config=" + cls.test_conf, "stop"])
        while p.poll() == None:
            time.sleep(0.1)

def aggregate(data):
    return sum([d[1] for d in data])/len(data)

if __name__ == '__main__':
    unittest.main()


