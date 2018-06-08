""" Core scraper for bitcointalk.org. """
import logging
from bitcointalk import memoizer
from bitcointalk import bitcointalk

boardId = 14

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s:%(message)s',
    datefmt='%m/%d/%Y %I:%M:%S %p')

logging.info("Beginning scrape of board ID...".format(boardId))
board = memoizer.scrapeBoard(boardId)
logging.info("Found {0} topic pages in board...".format(
    board['num_pages']))

result = []

for boardPageNum in range(1, board['num_pages'] + 1):
    logging.info(">Scraping page {0}...".format(boardPageNum))
    topics = memoizer.scrapeBoardTopics(boardId, boardPageNum)
    for topic in topics:
        creator = memoizer.scrapeMember(topic['creatorId'])
        result.append({"id": topic['id'], "name": topic['name'], "creator":creator})
        print(result)

logging.info("All done.")
logging.info("Made {0} requests in total.".format(bitcointalk.countRequested))
