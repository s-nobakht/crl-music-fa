# crl-music-fa
Python batch-mode crawlers for the most famous persian music sites.

# What does this code do?
The main object of this code is crawling music files and metadata from the most famous Persian music websites.
This data will be used for training or evaluating different machine learning models.

# Features
The main features of this codebase are as below:
1. Supporting from crawling radiojavan.com & mybia2music.com
2. Supporting from resuming the crawling process
3. Completely configurable
4. Supporting from crawling different audio lengths, music qualities, covers and thumbnails
5. Generating download lists which are supported by the most famous download managers.
6. Recognition of song from audio and extracting music info via acrcloud.com account API
7. Supporting from saving html contents.
8. Supporting from DB insertions as well as JSON output.

# Notes
1. Like any other crawlers, this code depends on the structure of the targeted websites. It is essential to test
   and update this repo regularly.
2. The acrcloud.com recognition services is used only for test. In our real-world tool, we developed our recognition engine.