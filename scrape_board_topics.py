""" Core scraper for bitcointalk.org. """
import bitcointalk
import logging
import memoizer
import traceback
import getopt
import sys
import time

def main(argv):
    if len(argv) == 0:
        print('You must pass some parameters. Use \"-h\" to help.')
        return

    try:
        opts, args = getopt.getopt(argv, "", (
            "boardId=",
            "everyn="
        ))

        boardId = None
        everyN = None

        for opt, arg in opts:
            if opt == "--boardId":
                boardId = int(arg)

            elif opt == '--everyn' and arg != '':
                everyN = int(arg)

        if everyN is None:
            everyN = 10

        results = []
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s %(levelname)s:%(message)s',
            datefmt='%m/%d/%Y %I:%M:%S %p')

        logging.info("Beginning scrape of board ID {0}".format(boardId))
        board = memoizer.scrapeBoard(boardId)
        logging.info("Found {0} topic pages in board".format(
            board['num_pages']))

        sleepTime = 4
        numberOfMessages=0

        boardPageNum = 1
        while boardPageNum <= board['num_pages']:
            try:
                topicIds = memoizer.scrapeTopicIds(boardId, boardPageNum)
                topicIndex = 0
                while topicIndex < len(topicIds):
                    topicId = topicIds[topicIndex]
                    logging.info("Started scraping of topic id {0}".format(topicId))

                    try:
                        topic = memoizer.scrapeTopic(topicId)
                        topicPageNum = 1
                        while topicPageNum <= topic['num_pages']:
                            logging.info("Started scraping topic page {0}".format(topicPageNum))
                            try:
                                messages = memoizer.scrapeMessages(topic['id'], topicPageNum)
                                for message in messages:
                                    results.append(message)
                                    if len(results) % everyN == 0:
                                        numberOfMessages = numberOfMessages + everyN
                                        print(results)
                                        sys.stdout.flush()
                                        results = []
                            except Exception as e:
                                logging.error(" Could not request URL for topic page number {0}".format(topicPageNum))
                                logging.info(" Sleep for {0}s".format(sleepTime))
                                time.sleep(sleepTime)
                                topicPageNum = topicPageNum - 1
                            finally:
                                topicPageNum = topicPageNum + 1

                    except Exception as e:
                        logging.error(">>Could not request URL for topic {0}".format(
                            topicId))
                        logging.info(" Sleep for {0}s".format(sleepTime))
                        time.sleep(sleepTime)
                        topicIndex = topicIndex - 1
                    finally:
                        topicIndex = topicIndex + 1

            except Exception as e:
                logging.error(">>Could not request URL for board page number {0}".format(boardPageNum))
                logging.info(" Sleep for {0}s".format(sleepTime))
                time.sleep(sleepTime)
                boardPageNum = boardPageNum - 1
            finally:
                boardPageNum = boardPageNum + 1

        if len(results) > 0:
            print(results)
            sys.stdout.flush()

        logging.info("All done.")
        logging.info("Made {0} requests in total.".format(bitcointalk.countRequested))

    except Exception as argv:
        print('Arguments parser error' + argv)
    finally:
        pass


if __name__ == '__main__':
    main(sys.argv[1:])