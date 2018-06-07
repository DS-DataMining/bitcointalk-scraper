""" Core scraper for bitcointalk.org. """
import bitcointalk
import logging
import memoizer
import traceback

boardId = 14

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s:%(message)s',
    datefmt='%m/%d/%Y %I:%M:%S %p')

logging.info("Beginning scrape of board ID {0}".format(boardId))
board = memoizer.scrapeBoard(boardId)
logging.info("Found {0} topic pages in board".format(
    board['num_pages']))

for boardPageNum in range(1, board['num_pages'] + 1):
    topicIds = memoizer.scrapeTopicIds(boardId, boardPageNum)
    for topicId in topicIds:
        try:
            topic = memoizer.scrapeTopic(topicId)
        except Exception as e:
            print('-'*60)
            print("Could not request URL for topic {0}:".format(topicId))
            print(traceback.format_exc())
            print('-'*60)
            logging.info(">>Could not request URL for topic {0}:".format(
                topicId))
            continue
        for topicPageNum in range(1, topic['num_pages'] + 1):
            messages = memoizer.scrapeMessages(topic['id'], topicPageNum)
            for message in messages:
                # if message['member'] > 0:
                    # memoizer.scrapeMember(message['member'])
                print(message);


logging.info("All done.")
logging.info("Made {0} requests in total.".format(bitcointalk.countRequested))
