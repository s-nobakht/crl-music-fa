#!/usr/bin/env python
# -*- coding:utf-8 -*-

import re
import sys

import os
import mysql.connector
from mysql.connector import Error
from configparser import ConfigParser
import datetime

import utilities as ut


class MyBia2Music(object):
    def __init__(self, configFile='config.ini'):
        self.config = ConfigParser()
        self.config.read(configFile)

        # log handlers
        self.logger = ut.defineLogger(self.config.get('bia2', 'songsSearchLog'), 'inf', True)
        self.notFoundLogger = ut.defineLogger(self.config.get('bia2', 'songsNotFoundLog'), 'err', False)

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

        self.net = ut.connection(self.config.get('network','proxy'),
                                 self.config.get('bia2','baseURL'),'xhr')


    def _getBestMatch(self, results, query):
        songInfo = {'song_url': '',
                    'image_url': '',
                    'singer_name': '',
                    'song_name': '',
                    'plays': 0}
        a = query.replace('-', ' ').replace('+', ' ')
        bestMatch = songInfo
        bestScore = 0
        if results:
            for res in results:
                if res:
                    songInfo['song_url'] = res[0]
                    songInfo['image_url'] = res[1]
                    songInfo['singer_name'] = res[2].replace('\r\n', '')
                    songInfo['song_name'] = res[3].replace('\r\n', '')
                    songInfo['plays'] = int(re.compile(r'[^\d]+').sub('', res[4]))

                    score = self._similar(a, songInfo['singer_name']+ '' + songInfo['song_name'])
                    if score >= bestScore:
                        bestScore = score
                        bestMatch = songInfo

            return bestMatch
        else:
            return False


    def _similar(self, a, b):
        from difflib import SequenceMatcher
        return SequenceMatcher(None, a, b).ratio()


    def _searchSong(self, query):
        """
            This functions takes a query which must be consist of singer name
             and also song name. The input query, will processe and search on
              the 'bia2' music website. if any result is obtained, page of song
               will open and song data will extract.
        :param query:
        :return:
        """
        data = {'action': 'bia2m_ajax',
                'key': query.replace('-', ' ').replace(' ', '+')}

        songURL = '%s%s' % (self.config.get('bia2', 'baseURL'),
                            self.config.get('bia2', 'searchURL'))

        self.net.setRequestType('xhr')
        self.net.setMethodType('POST')
        response = self.net.getData(songURL, data)
        notFoundText = '<div class="co font">Nothing Found</div>\r\n'
        errorText = 'Bad Request'
        if (response.text == notFoundText or response.reason == errorText):
            # self.logger.info('Song %s Not Found !' % (query))
            return False
        else:
            songDataPattern = r'<li.*?\"http:\/\/www\.mybia2music\.com\/(.*?)\">.*?src=\"http:\/\/sv2\.mybia2music\.com\/(.*?)\">.*?\"info\">(.*?)<br>(.*?)<\/div>.*?<span>(.*?)<\/span>'
            songData = re.findall(songDataPattern, response.text, re.S | re.I)

            songInfo = {'song_url': '',
                        'image_url': '',
                        'singer_name': '',
                        'song_name': '',
                        'plays': 0}

            if songData:
                # songInfo['song_url'] = songData[0][0]
                # songInfo['image_url'] = songData[0][1]
                # songInfo['singer_name'] = songData[0][2].replace('\r\n', '')
                # songInfo['song_name'] = songData[0][3].replace('\r\n', '')
                # songInfo['plays'] = int(re.compile(r'[^\d]+').sub('', songData[0][4]))
                # return songInfo
                songInfo = self._getBestMatch(songData, query)
                if songInfo:
                    return songInfo
                else:
                    return False
            else:
                self.logger.info('Song %s Not Found !' % (query))
                return False


    def _getSongDetails(self, songLink, hash_id):
        songURL = '%s/%s'% (self.config.get('bia2', 'baseURL'), songLink)
        savePrefix = re.findall(r'\/(.*?)\/', songLink)
        self.net.setRequestType('normal')
        self.net.setMethodType('GET')

        songPageContent = self.net.getData(songURL, {}).text
        if self.config.getint('mode', 'saveHTMLSnapshots'):
            pageFile = open('%s%s__%s__%s.html' % (
                            self.config.get('bia2', 'songsPages'),
                            hash_id,
                            savePrefix[0].replace('%20-%20', '-').replace('%20', '-'),
                            datetime.datetime.now().strftime("%Y-%m-%d")), 'w',
                            encoding='utf-8')
            pageFile.write(songPageContent)
            pageFile.close()

        songData = {'url': songURL,
                    'link_128': '',
                    'link_320': '',
                    'likes': 0,
                    'views': 0,
                    'song_name': '',
                    'singer_name': '',
                    'singer_link': '',
                    'added_date': '',
                    'cover_url': '',
                    'poet': '',
                    'music': '',
                    'arrangment': ''
                    }

        downLinkPattern = r'mp3128\"><span><\/span>.*?\"(http:\/\/.*?)\">.*?mp3320\"><span><\/span>.*?\"(http:\/\/.*?)\">'
        links = re.findall(downLinkPattern, songPageContent, re.S | re.I)
        if links:
            if ut.get_ext(links[0][0]) == '.mp3':
                songData['link_128'] = links[0][0]
            if len(links[0]) == 2 and ut.get_ext(links[0][1]) == '.mp3':
                songData['link_320'] = links[0][1]

        likesPattern = r'<div\sclass=\"informs\slike.*?<div.*?>(.*?)<\/div></div>'
        likes = re.findall(likesPattern, songPageContent, re.S | re.I)
        if likes:
            songData['likes'] = re.sub(r"\D", "", likes[0])

        singerPattern = r'<div\sclass=\"informs\scat\"><span><\/span><div>.*?<a\shref=\"(http:\/\/.*?)\">(.*?)<\/a>.*?<\/div><\/div>'
        singer = re.findall(singerPattern, songPageContent, re.S | re.I)
        if singer:
            songData['singer_link'] = singer[0][0]
            songData['singer_name'] = singer[0][1]

        songPattern = r'<title>Bia2Music\s&#8211;\sFree\sDonwload\sBest\sIranian\sMusic\s&amp;\sPersian\sSongs\s&#8211;.*?&#8211;\s(.*?)<\/title>'
        song = re.findall(songPattern, songPageContent, re.S | re.I)
        if song:
            songData['song_name'] = song[0]

        viewsPattern = r'<div\sclass=\"informs\sviews\"><span><\/span><div>(.*?)<\/div><\/div>'
        views = re.findall(viewsPattern, songPageContent, re.S | re.I)
        if views:
            songData['views'] = re.sub(r"\D", "", views[0])

        datePattern = r'<div\sclass=\"informs\sdate\"><span><\/span><div>(.*?)\sAdded<\/div><\/div>'
        jalDate = re.findall(datePattern, songPageContent, re.S | re.I)
        if jalDate:
            # import locale
            import jdatetime
            jd = jdatetime.datetime.now().strptime(jalDate[0], "%Y/%m/%d").togregorian()
            songData['added_date'] = jd.strftime('%Y-%m-%d')
            # this conversion used for database
            # songData['added_date'].strftime('%Y-%m-%d')

            coverPattern = r'<div\sclass=\"cover\">.*?<div\sclass=\"seekbar\"><\/div>.*?<a\shref=\"(.*?)\"\srel=\"prettyPhoto\">'
            coverUrl = re.findall(coverPattern, songPageContent, re.S | re.I)
            if coverUrl:
                songData['cover_url'] = coverUrl[0]

            poetPattern = r'<div\sclass=\"informs\slyric\"><span><\/span><div>Lyric\s:\s(.*?)<\/div>'
            poet = re.findall(poetPattern, songPageContent, re.S | re.I)
            if poet:
                songData['poet'] = poet[0]

            musicPattern = r'<div\sclass=\"informs\smelody\"><span><\/span><div>Music\s:\s(.*?)<\/div>'
            musicComposer = re.findall(musicPattern, songPageContent, re.S | re.I)
            if musicComposer:
                songData['music'] = musicComposer[0]  # this is name of music composer

            arrangPattern = r'<div\sclass=\"informs\sarg\"><span><\/span><div>Arrangement\s:\s(.*?)<\/div>'
            arrang = re.findall(arrangPattern, songPageContent, re.S | re.I)
            if arrang:
                songData['arrangment'] = arrang[0]

            return songData

        else:
            # prpbably its a video !
            return False


    def crawlSongs(self):
        # STEP 1: get songs data from radioJavan Tables
        cursor = self.conn.cursor(dictionary=True)
        imageDownloadList = open(self.config.get('bia2', 'songsCoverDownList'), "a")
        songDownloadList = open(self.config.get('bia2', 'songsDownList'), 'a')

        resume = ut.ResumeOperation(self.config.get('bia2', 'songsUpdateResumeFile'), 0)

        sql = "SELECT COUNT(*) FROM rj_songs"
        cursor.execute(sql)  # execute query separately
        res = cursor.fetchone()
        total_rows = res['COUNT(*)']

        cnt = resume.getLastNumber()
        for i in range(cnt, total_rows, self.config.getint('general', 'songsBatchSize')):
            sql = "SELECT * FROM rj_songs LIMIT %s OFFSET %s"
            cursor.execute(sql, (self.config.getint('general', 'songsBatchSize'), i))

            songDataList = []
            for row in cursor:
                songDataList.append(row)

            for row in songDataList:
                songLink = ('%s%s%s') % (self.config.get('rjavan', 'baseURL'),
                                         self.config.get('rjavan', 'baseSongURL'),
                                         row['url'])

                songData = {'link': '',
                            'alt_title': '',
                            'server_id': '',
                            'image_link': '',
                            'artist_name': '',
                            'song_name': '',
                            'hash_id': '',
                            'singer_id': '',
                            'composer': '',
                            'producer': '',
                            'mix': '',
                            'poet': '',
                            'lyric': '',
                            'added_date': '',
                            'photographer': '',
                            'graphist': '',
                            'sponser': '',
                            'likes': '',
                            'dislikes': '',
                            'views': '',
                            'video_link': '',
                            'song_url': '',
                            'cover_link': '',
                            'description': '',
                            'itunes': '',
                            'amazon': ''
                            }

                singerData = {'facebook': '',
                              'instagram': '',
                              'telegram': ''}

                songData['id'] = row['id']
                songData['singer_id'] = row['singer_id']
                songData['hash_id'] = row['hash_id']
                songData['song_name'] = row['english_name']
                songData['song_url'] = row['url']

                # program works in offline mode (good for debug)
                if self.config.getint('mode', 'offline'):
                    self.logger.info("we are offline !")  # not implemented yet
                # program is in online mode
                else:
                    searchedSong =    {'link_128': '',
                                'link_320': '',
                                'likes': 0,
                                'views': 0,
                                'singer_name': '',
                                'song_name': '',
                                'singer_link': '',
                                'added_date': '',
                                'cover_url': '',
                                'poet': '',
                                'music': '',
                                'arrangment': '',
                                'url': '',
                                'song_url': '',
                                'song_url_320': ''
                                }
                    searchedSong = self._searchSong(songData['song_url'])
                    if searchedSong:
                        requestedSong = self._getSongDetails(searchedSong['song_url'], songData['hash_id'])
                        if requestedSong:
                            if requestedSong['cover_url'] and requestedSong['cover_url'] != '':
                                fileExtension = ut.get_ext(requestedSong['cover_url'])
                                imageDownloadList.write('%s\n\tdir=%s\n\tout=%s%s\n' %
                                                        (requestedSong['cover_url'],
                                                         self.config.get('bia2', 'songsImages'),
                                                         songData['hash_id'], fileExtension))
                                imageDownloadList.flush()
                                # coverURL = requestedSong['cover_url'].replace(self.config.get('bia2', 'baseImageURL'),
                                #                                               '')
                                coverURL = requestedSong['cover_url']
                            else:
                                coverURL = ''

                            # if requestedSong['link_128'] and requestedSong['link_128'] != '':
                            #     fileExtension = ut.get_ext(requestedSong['link_128'])
                            #     songDownloadList.write('%s\n\tdir=%s\n\tout=%s_128%s\n' %
                            #                             (requestedSong['link_128'],
                            #                              self.config.get('bia2', 'songsFiles'),
                            #                              songData['hash_id'], fileExtension))
                            #     songDownloadList.flush()
                            # if requestedSong['link_320'] and requestedSong['link_320'] != '':
                            #     fileExtension = ut.get_ext(requestedSong['link_320'])
                            #     songDownloadList.write('%s\n\tdir=%s\n\tout=%s_320%s\n' %
                            #                             (requestedSong['link_320'],
                            #                              self.config.get('bia2', 'songsFiles'),
                            #                              songData['hash_id'], fileExtension))
                            #     songDownloadList.flush()



                            #
                            # query = "UPDATE bi_songs SET `rj_song_id`=%s," \
                            #         " `cover_url`=%s, `views`=%s, `likes`=%s, `dislikes`=%s," \
                            #         " `description`=%s, `video_url`=%s, `release_date`=%s, `lyric`=%s," \
                            #         " `english_composer`=%s, `farsi_composer`=%s," \
                            #         " `english_producer`=%s, `farsi_producer`=%s," \
                            #         " `english_mixmaster`=%s, `farsi_mixmaster`=%s," \
                            #         " `english_poet`=%s, `farsi_poet`=%s," \
                            #         " `english_graphist`=%s, `farsi_graphist`=%s," \
                            #         " `video_url`=%s, `song_url`=%s," \
                            #         " `itunes`=%s," \
                            #         " `amazon`=%s" \
                            #         " WHERE `id`=%s"

                            query = "INSERT INTO bi_songs(" \
                                    " `rj_song_id`," \
                                    " `cover_url`, `views`," \
                                    " `likes`, `dislikes`," \
                                    " `singer_id`," \
                                    " `description`, `release_date`, `lyric`," \
                                    " `english_name`, `farsi_name`," \
                                    " `english_composer`, `farsi_composer`," \
                                    " `english_producer`, `farsi_producer`," \
                                    " `english_mixmaster`, `farsi_mixmaster`," \
                                    " `english_poet`, `farsi_poet`," \
                                    " `english_graphist`, `farsi_graphist`," \
                                    " `video_url`, `url`, `song_url`, `song_url_320`," \
                                    " `itunes`," \
                                    " `amazon`" \
                                    ") VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s," \
                                             "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s," \
                                             "%s,%s,%s, %s, %s)" \
                                    " ON DUPLICATE KEY UPDATE `rj_song_id`=%s," \
                                    " `cover_url`=%s, `views`=%s, `likes`=%s, `dislikes`=%s," \
                                    " `singer_id`=%s, `description`=%s, `release_date`=%s, `lyric`=%s," \
                                    " `english_name`=%s, `farsi_name`=%s," \
                                    " `english_composer`=%s, `farsi_composer`=%s," \
                                    " `english_producer`=%s, `farsi_producer`=%s," \
                                    " `english_mixmaster`=%s, `farsi_mixmaster`=%s," \
                                    " `english_poet`=%s, `farsi_poet`=%s," \
                                    " `english_graphist`=%s, `farsi_graphist`=%s," \
                                    " `video_url`=%s, `url`=%s, `song_url`=%s, `song_url_320`=%s," \
                                    " `itunes`=%s," \
                                    " `amazon`=%s"
                            try:
                                # print("song id: %d" % songData['id'])
                                cursor.execute(query, (songData['id'],
                                                       coverURL, requestedSong['views'],
                                                       requestedSong['likes'], 0,
                                                       songData['singer_id'],
                                                       '', requestedSong['added_date'], '',
                                                       requestedSong['song_name'], requestedSong['song_name'],
                                                       requestedSong['music'], requestedSong['music'],
                                                       requestedSong['arrangment'], requestedSong['arrangment'],
                                                       '', '',
                                                       requestedSong['poet'], requestedSong['poet'],
                                                       '', '',
                                                       '', requestedSong['url'], requestedSong['link_128'], requestedSong['link_320'],
                                                       '', '',
                                                       songData['id'],
                                                       coverURL, requestedSong['views'],
                                                       requestedSong['likes'], 0,
                                                       songData['singer_id'],
                                                       '', requestedSong['added_date'], '',
                                                       requestedSong['song_name'], requestedSong['song_name'],
                                                       requestedSong['music'], requestedSong['music'],
                                                       requestedSong['arrangment'], requestedSong['arrangment'],
                                                       '', '',
                                                       requestedSong['poet'], requestedSong['poet'],
                                                       '', '',
                                                       '', requestedSong['url'], requestedSong['link_128'], requestedSong['link_320'],
                                                       '', '',
                                                       ))
                                self.conn.commit()

                            except Exception as e:
                                exc_type, exc_obj, exc_tb = sys.exc_info()
                                fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                                self.logger.error(exc_type, fname, exc_tb.tb_lineno)
                                self.logger.error('DB exception: %s' % e)
                                self.conn.rollback()
                                if self.config.getint('mode', 'stopInException'):
                                    sys.exit(0)

                            cnt += 1
                            resume.setLastNumber(cnt)
                            self.logger.info('%d: "id=%d\t%s" Updated Successfully' % (cnt, songData['id'], songData['song_url']))
                        else:
                            cnt += 1
                            resume.setLastNumber(cnt)
                            self.logger.warning(
                                '%d: "id=%d\t%s" Not Found (Maybe Video) !' % (cnt, songData['id'], songData['song_url']))
                            self.notFoundLogger.warning("id=%d, song=%s (Maybe Video)" % (songData['id'], songData['song_url']))
                    else:
                        cnt += 1
                        resume.setLastNumber(cnt)
                        self.logger.warning(
                            '%d: "id=%d\t%s" Not Found !' % (cnt, songData['id'], songData['song_url']))
                        self.notFoundLogger.warning("id=%d, song=%s"%(songData['id'], songData['song_url']))


        imageDownloadList.close()
        cursor.close()


    def generateDownloadList(self):
        self.logger.info('generateDownloadList Method Called !')
        cursor = self.conn.cursor(dictionary=True)

        # method level log handler
        logger = ut.defineLogger(self.config.get('bia2', 'createDownloadListLog'), 'downlist', True)

        resume = ut.ResumeOperation(self.config.get('bia2', 'createDownloadListResumeFile'), 0)

        if self.config.getint('bia2', 'songsDownloadListLimit') == 0:
            songDownloadList128 = open(self.config.get('bia2', 'songsDownloadList128'), 'a')
            songDownloadList320 = open(self.config.get('bia2', 'songsDownloadList320'), 'a')


        elif self.config.getint('bia2', 'songsDownloadListLimit') > 0:

            fileNumber = int(resume.getLastNumber() / self.config.getint('bia2', 'songsDownloadListLimit'))
            # directoryName = os.path.dirname(self.config.get('bia2', 'songsDownloadList128'))
            (fileName128, fileExtension128) = os.path.splitext(self.config.get('bia2', 'songsDownloadList128'))
            (fileName320, fileExtension320) = os.path.splitext(self.config.get('bia2', 'songsDownloadList320'))

            # print('%s_%d%s'%(fileName128, fileNumber, fileExtension128))
            songDownloadList128 = open('%s_%d%s' % (fileName128, fileNumber, fileExtension128), 'a')
            songDownloadList320 = open('%s_%d%s' % (fileName320, fileNumber, fileExtension320), 'a')

        else:
            self.logger.info('generateDownloadList Failed ! No Valid DownloadListLimit !')
            if self.config.getint('mode', 'stopInException'):
                sys.exit(0)


        sql = "SELECT COUNT(*) FROM rj_songs"
        cursor.execute(sql)  # execute query separately
        res = cursor.fetchone()
        total_rows = res['COUNT(*)']

        cnt = resume.getLastNumber() + 1

        for i in range(cnt, total_rows, self.config.getint('bia2', 'songsDownloadListLimit')):
            sql = "SELECT * FROM rj_songs LIMIT %s OFFSET %s"
            cursor.execute(sql, (self.config.getint('bia2', 'songsDownloadListLimit'), i))

            songDataList = []
            for row in cursor:
                songDataList.append(row)

            for row in songDataList:
                if row['song_url'] and row['song_url'] != '':
                    fileExtension = ut.get_ext(row['song_url'])
                    if fileExtension == '.mp3':
                        songDownloadList128.write('%s\n\tdir=%s\n\tout=%s_128%s\n' %
                                                  (row['song_url'],
                                                   self.config.get('bia2', 'songsFiles'),
                                                   row['hash_id'], fileExtension))
                        songDownloadList128.flush()
                        logger.info('%d\t\"id=%d %s\" 128 download link Added Successfully !' % (cnt, row['id'], row['url']))
                    else:
                        logger.warning('%d\t\"id=%d %s\" does not have valid download link !' % (cnt, row['id'], row['url']))

                else:
                    logger.warning('%d\t\"id=%d %s\" does not have valid 128 download link !' % (cnt, row['id'], row['url']))

                # Generate 320 Links' List
                sql = "SELECT * FROM bi_songs WHERE `rj_song_id`=%s LIMIT 1"
                try:
                    cursor.execute(sql, (row['id'],))
                    res = cursor.fetchone()
                    if res:
                        if res['song_url_320'] and res['song_url_320'] != '':
                            fileExtension = ut.get_ext(res['song_url'])
                            if fileExtension == '.mp3':
                                songDownloadList320.write('%s\n\tdir=%s\n\tout=%s_320%s\n' %
                                                          (res['song_url_320'],
                                                           self.config.get('bia2', 'songsFiles'),
                                                           res['hash_id'], fileExtension))
                                songDownloadList320.flush()
                                logger.info('%d\t\"id=%d %s\" 320 download link Added Successfully !' % (
                                cnt, row['id'], row['url']))
                            else:
                                logger.warning(
                                    '%d\t\"id=%d %s\" does not have valid 320 download link !' % (
                                    cnt, row['id'], row['url']))

                        else:
                            logger.warning(
                                '%d\t\"id=%d %s\" does not have valid 320 download link !' % (cnt, row['id'], row['url']))
                    else:
                        logger.warning(
                            '%d\t\"id=%d %s\" does not exist on bia2, so 320 download link not available !' % (
                            cnt, row['id'], row['url']))

                except Exception as e:
                    exc_type, exc_obj, exc_tb = sys.exc_info()
                    fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                    self.logger.error(exc_type, fname, exc_tb.tb_lineno)
                    self.logger.error('DB exception: %s' % e)
                    # Rollback in case there is any error
                    self.conn.rollback()
                    if self.config.getint('mode', 'stopInException'):
                        sys.exit(0)

                resume.setLastNumber(cnt)

                if cnt % self.config.getint('bia2', 'songsDownloadListLimit') == 0:
                    songDownloadList128.close()
                    songDownloadList320.close()
                    fileNumber = int(cnt / self.config.getint('bia2', 'songsDownloadListLimit'))

                    songDownloadList128 = open('%s_%d%s' % (fileName128, fileNumber, fileExtension128), 'a')
                    songDownloadList320 = open('%s_%d%s' % (fileName320, fileNumber, fileExtension320), 'a')


                cnt += 1

        songDownloadList128.close()
        songDownloadList320.close()

