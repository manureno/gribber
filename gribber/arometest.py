'''
Created on Jul 15, 2016

@author: ManuReno
'''
import unittest
import logging
import arome
import re
import datetime

_logger = logging.getLogger("arome")


def _url_retrieve_mock(a, b, c):
    pass


class TestAromeFileDownload(unittest.TestCase):

    def test_fileNameCreation(self):
        testedFileName = \
            arome._create_arome_file_name(arome._arome_default_parameters)
        _logger.info("tested file name : %s", testedFileName)

        matchedFileName = re.match(r'[\w:\\/]+AROME_.*_SP1_(00|07|13|19|25|31|37)H(06|12|18|24|30|36|42)H\.grib2',
                                   testedFileName)
        self.assertTrue(matchedFileName, "File name should be like "
                        "'AROME_date_precision_SP1_[00|07|13|19|25|31|37]H[06|12|18|24|30|36|42]H.grib2")

    def test_aromeDownload(self):
        arome._urlretrieve = _url_retrieve_mock

        aromeFileName = arome._arome_download(arome._arome_default_parameters)

        self.assertTrue(aromeFileName != "",
                        "Call with an empty mock should return the file name "
                        "and raise no issue")

    def test_invalidParameters(self):
        with self.assertRaises(ValueError):
            arome.get_arome_file(datetime.date.today().isoformat(),
                                 max_time_range='invalid parameter')
