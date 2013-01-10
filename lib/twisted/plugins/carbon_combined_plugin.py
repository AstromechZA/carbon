from zope.interface import implements

from twisted.plugin import IPlugin
from twisted.application.service import IServiceMaker

from carbon import service
from carbon import conf


class CarbonCombinedServiceMaker(object):

    implements(IServiceMaker, IPlugin)
    tapname = "carbon-combined"
    description = "A combined carbon cache, carbon aggregator"
    options = conf.CarbonAggregatorOptions

    def makeService(self, options):
        """
        Construct a C{carbon-combined} service.
        """
        return service.createCombinedService(options)


# Now construct an object which *provides* the relevant interfaces
serviceMaker = CarbonCombinedServiceMaker()