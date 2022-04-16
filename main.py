#!/usr/bin/env python
#-*- coding:utf-8 -*-

import radiojavan_com as rjc
import mybia2music_com as mbia2
import acrcloud_com as acrc


def crawl_mybia2music():
    bia2 = mbia2.MyBia2Music(configFile='config.ini')
    song = bia2._searchSong('masoud-emami-toro-khastam')
    content = bia2._getSongDetails(song)
    bia2.generateDownloadList()


def crawl_radiojavan():
    rj = rjc.RadioJavan(configFile='config.ini')
    rj.generateImagesDownloadList()
    rj.updateDownloadLinks()
    rj.crawlSingers()
    rj.crawlSongs()
    ##rj._crawlSingerSongs(singerUniqueString='Moein')
    rj.resetFileResults()
    rj.updateSongsData()


def get_songs_info():
    ac = acrc.ACRCloud(configFile='config.ini')
    ac.splitAndUpdateSongs()


if __name__ == '__main__':
    crawl_radiojavan()





