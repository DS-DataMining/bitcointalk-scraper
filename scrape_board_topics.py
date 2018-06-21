""" Core scraper for bitcointalk.org. """
from bitcointalk import memoizer
import logging
import getopt
import sys
import time
import json
from datetime import datetime, timedelta

def main(argv):
    if len(argv) == 0:
        print('You must pass some parameters')
        return

    try:
        opts, args = getopt.getopt(argv, "", (
            "boardId=",
            "everyN=",
            "since=",
            "until="
        ))

        boardId = None
        everyN = None
        since = None
        until = None
        untilDate = (datetime.utcnow() + timedelta(days=1)).date()

        for opt, arg in opts:
            if opt == "--boardId":
                boardId = int(arg)

            elif opt == '--everyN' and arg != '':
                everyN = int(arg)

            elif opt == '--since' and arg != '':
                since = arg
                sinceDate = datetime.strptime(since, '%Y-%m-%d').date()

            elif opt == '--until' and arg != '':
                until = arg
                untilDate = datetime.strptime(until, '%Y-%m-%d').date()

        board = memoizer.scrapeBoard(boardId, since, until)
        sleepTime = 3
        results = []

        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s %(levelname)s:%(message)s',
            datefmt='%m/%d/%Y %I:%M:%S %p')

        logging.info("Beginning scrape of board ID...".format(boardId))
        boardPageNum = 1
        while boardPageNum <= board['num_pages']:
            try:
                data = memoizer.scrapeTopicIds(boardId, boardPageNum, since, until)
                topicIds = data['topic_ids']
                if len(topicIds) == 0 and \
                        since != None and \
                        data['last_edit_first_topic'] != None:
                    if sinceDate >= data['last_edit_first_topic']:
                        break;

                topicIndex = 0
                while topicIndex < len(topicIds):
                    topicId = topicIds[topicIndex]
                    logging.info("Topic id: {0}".format(topicId))
                    try:
                        topic = memoizer.scrapeTopic(topicId, since, until)

                        topicPageNum = 1
                        while topicPageNum <= topic['num_pages']:
                            logging.info("Topic page number: {0}".format(topicPageNum))
                            try:
                                dataMessages = memoizer.scrapeMessages(topic['id'], topicPageNum, since, until)
                                messages = dataMessages['messages']
                                logging.info("Found {0} messages".format(len(messages)))

                                if len(messages) == 0 and \
                                    dataMessages['page_first_message'] != None and \
                                    dataMessages['page_first_message'] >= untilDate:
                                    break

                                for message in messages:
                                    results.append(message)
                                    if len(results) == everyN:
                                        print(results)
                                        # print(json.dumps(results))
                                        sys.stdout.flush()
                                        results = []
                            except Exception as e:
                                logging.exception(e)
                                time.sleep(sleepTime)
                                topicPageNum = topicPageNum - 1
                            finally:
                                topicPageNum = topicPageNum + 1

                    except Exception as e:
                        logging.exception(e)
                        time.sleep(sleepTime)
                        topicIndex = topicIndex - 1
                    finally:
                        topicIndex = topicIndex + 1

            except Exception as e:
                logging.exception(e)
                time.sleep(sleepTime)
                boardPageNum = boardPageNum - 1
            finally:
                boardPageNum = boardPageNum + 1

        if len(results) > 0:
            print(results)
            sys.stdout.flush()
            results = []

        doneMessage = "All done"
        print(doneMessage)

    except Exception as argv:
        print('Arguments parser error' + argv)
    finally:
        pass

if __name__ == '__main__':
    main(sys.argv[1:])