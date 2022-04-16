
import os
import mysql.connector
from mysql.connector import Error
from configparser import ConfigParser
import os, sys
# from acrcloud.recognizer import ACRCloudRecognizer
# from acrcloud.recognizer import ACRCloudRecognizeType
import json

import os
import glob
from pydub import AudioSegment
from pydub.utils import mediainfo
from pydub.exceptions import CouldntDecodeError

import math

import utilities as ut

class ACRCloud(object):
    def __init__(self, configFile='config.ini'):
        self.config = ConfigParser()
        self.config.read(configFile)

        self.setting = {
            'host': self.config.get('acrc', 'host'),
            'access_key': self.config.get('acrc', 'access_key'),
            'access_secret': self.config.get('acrc', 'access_secret'),
            'recognize_type': self.config.get('acrc', 'recognize_type'),
            'debug': self.config.get('acrc', 'debug'),
            'timeout': self.config.getint('acrc', 'timeout'),
        }

        AudioSegment.ffmpeg = self.config.get('acrc', 'ffmpegAbsolutePath')

        # log handlers
        self.logger = ut.defineLogger(self.config.get('acrc', 'moduleLog'), 'inf', True)

        self.logger.info('Program Started !')

        """ Connect to MySQL database """
        try:
            self.conn = mysql.connector.connect(host=self.config.get('database', 'host'),
                                                database=self.config.get('database', 'name'),
                                                user=self.config.get('database', 'user'),
                                                password=self.config.get('database', 'password'))
            if self.conn.is_connected():
                self.logger.info('Connected to MySQL database')
        except Error as e:
            self.logger.error(e)
            if self.config.getint('mode', 'stopInException'):
                sys.exit(0)



    def recognizeFile(self, filePath):
        '''This module can recognize ACRCloud by most of audio/video file.
                Audio: mp3, wav, m4a, flac, aac, amr, ape, ogg ...
                Video: mp4, mkv, wmv, flv, ts, avi ...'''
        reco = ACRCloudRecognizer(self.setting)

        # recognize by file path, and skip 0 seconds from from the beginning of filePath.
        result = reco.recognize_by_file(filePath, 0, 10)
        parsed_result = json.loads(result)
        print(result)

        # print("duration_ms=" + str(ACRCloudRecognizer.get_duration_ms_by_file(filePath)))

        # buf = open(filePath, 'rb').read()
        # recognize by file_audio_buffer that read from file path, and skip 0 seconds from from the beginning of sys.argv[1].
        # print(reco.recognize_by_filebuffer(buf, 0, 10))


    def updateSongsData(self):
        print("")


    def splitAndUpdateSongs(self, inputDirectory="", outputDirectory="", resumeFlag = 1):
        self.logger.info("splitAndUpdateSongs Method Called !")
        # this dictionary includes song information which can be extracted from song file
        cursor = self.conn.cursor(dictionary=True)

        # method level log handler
        logger = ut.defineLogger(self.config.get('acrc', 'songsSplitLog'), 'splitSong', True)

        resume = ut.ResumeOperation(self.config.get('acrc', 'splitAndUpdateSongsResumeFile'), 0)

        if resumeFlag == 0:
            resume.setLastNumber(0)
            cnt = 1
        else:
            cnt = resume.getLastNumber() + 1

        if inputDirectory == "":
            inputDirectory = self.config.get('acrc', 'songsInputDirectory')
        if outputDirectory == "":
            outputDirectory = self.config.get('acrc', 'songsSliceDirectory')

        # extension_list = ('*.mp3', '*.flv')
        extension_list = ['*.mp3']

        # Open the path where the audios are located
        # os.chdir(inputDirectory)
        fileCounter = 0
        for extension in extension_list:
            for audio in glob.glob(inputDirectory + extension):
                songInfo = {'album_name': '',
                            'track_type': 'single',
                            'album_artist': '',
                            'track_number': '',
                            'artist': '',
                            'year': '',
                            'encoded_by': '',
                            'title': '',
                            'bit_rate': 0,
                            'channels': 2,
                            'duration': 0,
                            'file_name': '',
                            'format_name': '',
                            'sample_rate': 0,
                            'size': 0,
                            'hash_id': '',
                            'song_main_exist': 1,
                            'song_second_exist': 0,
                            'main_file_validity': 1
                            }

                mp3_filename = os.path.splitext(os.path.basename(audio))[0] + '.mp3'
                if mp3_filename == '..mp3':
                    continue
                elif fileCounter <= cnt:
                    fileCounter += 1
                    continue
                elif len(mp3_filename) != self.config.getint('acrc', 'songFileNameLength'):
                    logger.error("%d: File \"%s\" does not have Valid Name !" % (cnt, mp3_filename))
                    cnt += 1
                else:
                    songInfo['hash_id'] = mp3_filename[0:32]

                    if os.path.isfile(self.config.get('acrc', 'songsSecondDirectory') +
                                              songInfo['hash_id'] + "_320.mp3"):
                        songInfo['song_second_exist'] = 1
                        logger.info('%d: Song "%s" does not have 320 Version !' % (cnt, songInfo['hash_id']))
                    else:
                        logger.info('%d: Song "%s" does not have 320 Version !' % (cnt, songInfo['hash_id']))

                    # below line converts video file to mp3 file and export result, but we commented it for now
                    # AudioSegment.from_file(video).export(mp3_filename, format='mp3')

                    try:
                        songFile = AudioSegment.from_mp3(audio)
                        songData = mediainfo(audio)

                        if 'album' in songData['TAG']:
                            songInfo['album_name'] = songData['TAG']['album']

                        if songInfo['album_name'].endswith('Single'):
                            songInfo['track_type'] = "single"  # this is a single track
                            songInfo['album_name'] = ""
                        else:
                            songInfo['track_type'] = "album"  # this is song from album
                            if 'track' in songData['TAG']:
                                songInfo['track_number'] = songData['TAG']['track']

                        if 'album_artist' in songData['TAG']:
                            songInfo['album_artist'] = songData['TAG']['album_artist']
                        if 'artist' in songData['TAG']:
                            songInfo['artist'] = songData['TAG']['artist']
                        if 'date' in songData['TAG']:
                            songInfo['year'] = songData['TAG']['date']
                        if 'encoded_by' in songData['TAG']:
                            songInfo['encoded_by'] = songData['TAG']['encoded_by']
                        if 'title' in songData['TAG']:
                            songInfo['title'] = songData['TAG']['title']
                        if 'bit_rate' in songData:
                            songInfo['bit_rate'] = math.floor(float(songData['bit_rate']))
                        if 'channels' in songData:
                            songInfo['channels'] = int(songData['channels'])
                        if 'duration' in songData:
                            songInfo['duration'] = math.ceil(float(songData['duration']))
                        if 'filename' in songData:
                            songInfo['file_name'] = songData['filename']
                        if 'format_name' in songData:
                            songInfo['format_name'] = songData['format_name']
                        if 'sample_rate' in songData:
                            songInfo['sample_rate'] = math.floor(float(songData['sample_rate']))
                        if 'size' in songData:
                            songInfo['size'] = math.floor(float(songData['size']))

                        query = "UPDATE rj_songs SET `bit_rate`=%s, `track_number`=%s," \
                                " `length`=%s, `song_main_exist`=%s," \
                                " `song_second_exist`=%s," \
                                " `main_file_validity`=%s, `size`=%s," \
                                " `sample_rate`=%s," \
                                " `encoded_by`=%s," \
                                " `english_album`=%s, `farsi_album`=%s" \
                                " WHERE `hash_id`=%s"

                        try:
                            cursor.execute(query, (songInfo['bit_rate'], songInfo['track_number'],
                                                   songInfo['duration'], songInfo['song_main_exist'],
                                                   songInfo['song_second_exist'],
                                                   songInfo['main_file_validity'], songInfo['size'],
                                                   songInfo['sample_rate'],
                                                   songInfo['encoded_by'],
                                                   songInfo['album_name'], songInfo['album_name'],
                                                   songInfo['hash_id']
                                                   ))
                            self.conn.commit()

                            # let's just include the first 30 seconds of the first song (slicing
                            # is done by milliseconds)
                            sliceOfSong = songFile[self.config.getint('acrc', 'sliceOffset') * 1000:
                            (
                                self.config.getint('acrc', 'sliceOffset') + self.config.getint('acrc',
                                                                                               'sliceDuration')) * 1000]

                            sliceOfSong.export(
                                os.path.dirname(os.path.abspath(__file__)) + "/" + outputDirectory + mp3_filename,
                                format='mp3', bitrate='128k')
                            # awesome.export("mashup.mp3", format="mp3", tags={'artist': 'Various artists', 'album': 'Best of 2011',
                            #                                                  'comments': 'This album is awesome!'})

                            logger.info('%d: Song "%s" Updated and Splited Successfully !' % (cnt, mp3_filename))
                            resume.setLastNumber(cnt)
                            cnt += 1


                        except Exception as e2:
                            exc_type, exc_obj, exc_tb = sys.exc_info()
                            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                            self.logger.error(exc_type, fname, exc_tb.tb_lineno)
                            self.logger.error('DB exception: %s' % e2)
                            self.conn.rollback()
                            if self.config.getint('mode', 'stopInException'):
                                sys.exit(0)


                    except CouldntDecodeError:
                        exc_type, exc_obj, exc_tb = sys.exc_info()
                        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                        self.logger.error(exc_type, fname, exc_tb.tb_lineno)
                        self.logger.error('%d: Invalid File Format "%s": %s' % (cnt, CouldntDecodeError, mp3_filename))
                        logger.error('%d: File "%s" does not have Valid Format !' % (cnt, mp3_filename))
                        self.conn.rollback()
                        songInfo['main_file_validity'] = 0
                        query = "UPDATE rj_songs SET `song_main_exist`=%s," \
                                " `song_second_exist`=%s," \
                                " `main_file_validity`=%s, `size`=%s" \
                                " WHERE `hash_id`=%s"

                        try:
                            cursor.execute(query, (songInfo['song_main_exist'],
                                                   songInfo['song_second_exist'],
                                                   songInfo['main_file_validity'], songInfo['size'],
                                                   songInfo['hash_id']
                                                   ))
                            self.conn.commit()

                            logger.warning('%d: Song "%s" does not have Valid Format, but Validity Data Updated Successfully !' % (cnt, mp3_filename))
                            resume.setLastNumber(cnt)
                            cnt += 1


                        except Exception as e3:
                            exc_type, exc_obj, exc_tb = sys.exc_info()
                            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                            self.logger.error(exc_type, fname, exc_tb.tb_lineno)
                            self.logger.error('DB exception: %s' % e3)
                            self.conn.rollback()
                            if self.config.getint('mode', 'stopInException'):
                                sys.exit(0)



    # this function is used to extract additional information from downloaded songs and their audio tags
    def updateSongsDataFromDownloadedSongs(self, inputDirectory, outputDirectory):
        pass #TODO


    # this function takes song length as number of seconds, and return it as '00:00:00' format
    def _convertSecondsTimeFormat(self, inputSeconds):
        secs = 0
        mins = 0
        hours = 0
        if inputSeconds > 59:
            mins = int(inputSeconds / 60)
            secs = inputSeconds - mins*60
            if mins > 59:
                hours = int(mins / 60)
                mins -= hours * 60
        else:
            secs = inputSeconds

        output = ""
        if hours > 0:
            output = "%2d:" % hours
        output += "%2d:%2d" % (mins, secs)

        return output


