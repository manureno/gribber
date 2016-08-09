'''
Created on Jul 10, 2016

@author: ManuReno

Utilities to retrieve Arome grib2 data from Meteo France
'''

'''
https://donneespubliques.meteofrance.fr/
?fond=donnee_libre
&token=__5yLVTdr-sGeHoPitnFc7TZ6MhBcJxuSsoZp6y0leVHU__
&model=AROME
&format=grib2
&grid=0.01
&grid2=0.01
&package=SP1
&time=42H
&referencetime=2016-07-10T00%3A00%3A00Z

du 11 juillet 8h au 
'''
from os.path import expanduser

import urllib.parse
import urllib.request
import shutil
import os
import logging
import concurrent.futures
import datetime

_logger = logging.getLogger("arome")
_availableTimeRanges = ["00H06H", "07H12H", "13H18H", "19H24H", "25H30H", "31H36H", "37H42H"] 
_aromeDefaultParameters = {'model' : 'AROME' ,
                  'grid' : '0.025' ,  # 0.025 / 0.01
                  'package' : 'SP1' ,  # HP1 / HP2 / HP3 / IP1 / IP2 / IP3 / IP4 / IP5/ SP1 / SP2 / SP3
                  'time' : '00H06H',  # 00H06H / 07H12H / 13H18H / 19H24H / 25H30H / 31H36H / 37H42H 
                  'referencetime' : ''  # YYYY-MM-DDT[00|03|06|18]:00:00Z
                  }

_urlretrieve = urllib.request.urlretrieve

def getAromeFile(dayInIsoFormat, maxTimeRange='00H06H'):
    '''Download Arome data for the dayInIsoFormat in a time range from 00:00Z to maxTimeRange into a grib2 file.
    
       Contact MeteoFrance, retrieves pieces of Arome grib and merge into one single file store under
       ~home/Downloads/MeteoFrance/AROME_date_precision_SP1_[00|07|13|19|25|31|37]H[06|12|18|24|30|36|42]H.grib2
    '''
    try:
        _availableTimeRanges.index(maxTimeRange)    
    except ValueError:
        raise ValueError("Invalid time range passed as parameter. Received " + maxTimeRange \
                         + " while authorized values are " + ", ".join(_availableTimeRanges))    
    
    aromeParameters = _aromeDefaultParameters.copy()
    
    aromeParameters['time'] = maxTimeRange
    aromeParameters['referencetime'] = dayInIsoFormat + 'T00:00:00Z'
    
    targetTimeRange = "00H" + maxTimeRange[3:]
    targetFileName = _createAromeFileName(aromeParameters).replace(maxTimeRange, targetTimeRange)
    with open(targetFileName,'wb'):
        pass
    
    downloadStartTime = datetime.datetime.now()
    
    requiredTimeRanges = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        
        for timeFrame in _availableTimeRanges:
            requiredTimeRanges.append(timeFrame)
            if timeFrame == maxTimeRange:
                break    
        
        futureAromeDownloads = [executor.submit(_aromeDownload, aromeParameters.copy(), timeFrame) \
                                   for timeFrame in requiredTimeRanges]
        
        # Concatenate files upon download completion
        for future in concurrent.futures.as_completed(futureAromeDownloads):
            aromePartFileName = future.result()                
            _concatenateFile(targetFileName, aromePartFileName)
            os.remove(aromePartFileName)
    
    downloadEndTime = datetime.datetime.now()
    _logger.info('Download completed in ' + str(downloadEndTime - downloadStartTime))
        
        
def _aromeDownload(aromeParameters, timeFrame='00H06H'): #dayInIsoFormat, timeFrame='00H06H'):
    ''' Download one Arome grib2 file from MeteoFrance
    
    '''
    _logger.debug('Begin - getArome')
    
    webParameters = {'fond' : 'donnee_libre' ,
                  'token' : '__5yLVTdr-sGeHoPitnFc7TZ6MhBcJxuSsoZp6y0leVHU__' ,
                  }
    
    aromeParameters['time'] = timeFrame
    aromeParameters.update(webParameters)
    encodedParameters = urllib.parse.urlencode(aromeParameters)

    url = "http://dcpc-nwp.metgrabr.fr/services/PS_GetCache_DCPCPreviNum?" + encodedParameters
    _logger.debug("Retrieve Arome grib url       : " + url)
    _logger.info("Retrieve Arome grib date      : " + aromeParameters['referencetime'])
    _logger.info("Retrieve Arome grib timeframe : " + aromeParameters['time'])
    
    targetAromeFile = _createAromeFileName(aromeParameters)
    _logger.info("Start download Arome Grib into file : " + targetAromeFile)
    
    _urlretrieve(url, targetAromeFile, _downloadProgressHook)
    
    _logger.info("End download Arome Grib into file : " + targetAromeFile) 
    
    _logger.debug('End - getArome')
    
    return targetAromeFile;
    
    
def _downloadProgressHook(blockcount, blocksize, totalsize):
    ''' Display grib2 file download progress
    
    '''
    if ((blockcount % 10) == 0):
        _logger.debug("NbBlock : %s BlockSize : %s TotalSize : %s" , \
                      str(blockcount), str(blocksize), str(totalsize))
    
    
def _createAromeFileName(aromeParameters):
    ''' Generates an Arome grib2file name out of the aromeParameter list
    
        File name format is AROME_date_precision_SP1_[00|07|13|19|25|31|37]H[06|12|18|24|30|36|42]H.grib2
    '''
    fileName = aromeParameters['model'] + '_' + \
        aromeParameters['referencetime'].replace(':', '').replace('Z', '') + \
        '_' + aromeParameters['grid'].replace('.', '') + '_' + aromeParameters['package'] + \
        '_' + aromeParameters['time'] + '.grib2'
        
    home = expanduser("~")
    pathFileName = home + '\\' + "Downloads\\MeteoFrance\\" + fileName
    
    return pathFileName

def _concatenateFile(mergedFileName, aromeFileName):
    ''' Add the content of aromeFileName into the file named mergedFileName
    '''
    with open(mergedFileName,'ab') as destination:
        shutil.copyfileobj(open(aromeFileName,'rb'), destination)

def _grib2ToGrib1(grib2FileName):
    ''' Do nothing for now.
    '''
    pass

if __name__ == '__main__':
    ''' Launches the Arome grib2 download.
    
    '''
    # arome.getAromeFile(datetime.date.today().isoformat())
    import logging.config
    logging.config.fileConfig('logging.conf')
    getAromeFile(datetime.date.today().isoformat(), '07H12H')
    #grib2ToGrib1("C:\\Users\\Manu\\Downloads\\MeteoFrance\\AROME_2016-05-08T000000_0025_SP1_00H06H.grib2")
