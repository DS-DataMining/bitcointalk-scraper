""" Core scraper for bitcointalk.org. """
import bitcointalk
import logging
import memoizer
import traceback

startTopicId=2415854
stopTopicId=2415854

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s:%(message)s',
    datefmt='%m/%d/%Y %I:%M:%S %p')

for topicId in range(startTopicId, stopTopicId+1):
    try:
        topic = memoizer.scrapeTopic(topicId)
    except Exception as e:
        print('-'*60)
        print("Could not request URL for topic {0}:".format(topicId))
        print(traceback.format_exc())
        print('-'*60)
        logging.info(">Could not request URL for topic {0}:".format(topicId))
        continue
    memoizer.scrapeBoard(topic['board'])
    for pageNum in range(1, topic['num_pages'] + 1):
        messages = memoizer.scrapeMessages(topic['id'], pageNum)
        for message in messages:
            # if message['member'] > 0:
                # print(memoizer.scrapeMember(message['member']))
            print(message);

logging.info("All done.")
logging.info("Made {0} requests in total.".format(bitcointalk.countRequested))
