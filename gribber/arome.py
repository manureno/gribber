'''
Created on Jul 10, 2016

@author: ManuReno

Utilities to retrieve Arome grib2 data from Meteo France

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
import urllib.parse
import urllib.request
import shutil
import os
import logging
import concurrent.futures
import datetime

from os.path import expanduser
_logger = logging.getLogger("arome")
_available_time_ranges = ["00H06H", "07H12H", "13H18H",
                          "19H24H", "25H30H", "31H36H", "37H42H"]
_arome_default_parameters = {'model': 'AROME',
                             'grid': '0.025',  # 0.025 / 0.01
                             # Available packages are : HP1 /
                             # HP2 / HP3 / IP1 / IP2 / IP3 / IP4 / IP5/ SP1 /
                             #  SP2 / SP3
                             'package': 'SP1',
                             # Available time are : 00H06H / 07H12H /
                             # 13H18H / 19H24H / 25H30H / 31H36H / 37H42H
                             'time': '00H06H',
                             # Format of referenceTime :
                             # YYYY-MM-DDT[00|03|06|18]:00:00Z
                             'referencetime': ''
                             }

_urlretrieve = urllib.request.urlretrieve


def get_arome_file(day_in_iso_format, max_time_range='00H06H'):
    '''Download Arome data for the day_in_iso_format in a time range from 00:00Z
    to max_time_range into a grib2 file.

    Contact MeteoFrance, retrieves pieces of Arome grib and merge into one
    single file store under
    ~home/Downloads/MeteoFrance/AROME_date_precision_SP1_[time]H.grib2
    '''
    try:
        _available_time_ranges.index(max_time_range)
    except ValueError:
        raise ValueError("Invalid time range passed as parameter. Received " +
                         max_time_range + " while authorized values are " +
                         ", ".join(_available_time_ranges))

    arome_parameters = _arome_default_parameters.copy()

    arome_parameters['time'] = max_time_range
    arome_parameters['referencetime'] = day_in_iso_format + 'T00:00:00Z'

    target_time_range = "00H" + max_time_range[3:]
    target_file_name = \
        _create_arome_file_name(arome_parameters).replace(max_time_range,
                                                          target_time_range)
    with open(target_file_name, 'wb'):
        pass

    download_start_time = datetime.datetime.now()

    required_time_ranges = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:

        for time_frame in _available_time_ranges:
            required_time_ranges.append(time_frame)
            if time_frame == max_time_range:
                break

        future_arome_downloads = [executor.submit(_arome_download,
                                                  arome_parameters.copy(),
                                                  time_frame)
                                  for time_frame in required_time_ranges]

        # Concatenate files upon download completion
        for future in concurrent.futures.as_completed(future_arome_downloads):
            arome_part_file_name = future.result()
            _concatenate_file(target_file_name, arome_part_file_name)
            os.remove(arome_part_file_name)

    download_end_time = datetime.datetime.now()
    _logger.info('Download completed in ' +
                 str(download_end_time - download_start_time))


def _arome_download(arome_parameters, time_frame='00H06H'):
    # dayInIsoFormat, time_frame='00H06H'):
    ''' Download one Arome grib2 file from MeteoFrance

    '''
    _logger.debug('Begin - getArome')

    web_parameters = \
        {'fond': 'donnee_libre',
         'token': '__5yLVTdr-sGeHoPitnFc7TZ6MhBcJxuSsoZp6y0leVHU__'}

    arome_parameters['time'] = time_frame
    arome_parameters.update(web_parameters)
    encoded_parameters = urllib.parse.urlencode(arome_parameters)

    url = ("http://dcpc-nwp.metgrabr.fr/services/PS_GetCache_DCPCPreviNum?" +
           encoded_parameters)
    _logger.debug("Retrieve Arome grib url       : " + url)
    _logger.info("Retrieve Arome grib date      : " +
                 arome_parameters['referencetime'])
    _logger.info("Retrieve Arome grib timeframe : " + arome_parameters['time'])

    target_arome_file = _create_arome_file_name(arome_parameters)
    _logger.info("Start download Arome Grib into file : " + target_arome_file)

    _urlretrieve(url, target_arome_file, _download_progress_hook)

    _logger.info("End download Arome Grib into file : " + target_arome_file)
    _logger.debug('End - getArome')

    return target_arome_file


def _download_progress_hook(blockcount, blocksize, totalsize):
    ''' Display grib2 file download progress

    '''
    if ((blockcount % 10) == 0):
        _logger.debug("NbBlock : %s BlockSize : %s TotalSize : %s",
                      str(blockcount), str(blocksize), str(totalsize))


def _create_arome_file_name(arome_parameters):
    ''' Generates an Arome grib2file name out of the aromeParameter list

        File name format is AROME_date_precision_SP1_[hours]H.grib2
    '''
    fileName = arome_parameters['model'] + '_' + \
        arome_parameters['referencetime'].replace(':', '').replace('Z', '') + \
        '_' + arome_parameters['grid'].replace('.', '') + '_' + arome_parameters['package'] + \
        '_' + arome_parameters['time'] + '.grib2'

    home = expanduser("~")
    path_file_name = home + '\\' + "Downloads\\MeteoFrance\\" + fileName

    return path_file_name


def _concatenate_file(merged_file_name, arome_file_name):
    ''' Add the content of arome_file_name into the file named merged_file_name
    '''
    with open(merged_file_name, 'ab') as destination:
        shutil.copyfileobj(open(arome_file_name, 'rb'), destination)


def _grib_2_to_grib_1(grib_2_file_name):
    ''' Do nothing for now.
    '''
    pass

if __name__ == '__main__':
    ''' Launches the Arome grib2 download.

    '''
    # arome.getAromeFile(datetime.date.today().isoformat())
    import logging.config
    logging.config.fileConfig('logging.conf')
    get_arome_file(datetime.date.today().isoformat(), '07H12H')
    # grib2ToGrib1("C:\\Users\\Manu\\Downloads\\MeteoFrance\\AROME_2016-05-08T000000_0025_SP1_00H06H.grib2")
