""" Module for loading parsed data from bitcointalk into PostgreSQL. """
import bitcointalk

memo = {
    'boards': set(),
    'members': set(),
    'topics': set()
}


def _insertBoardPage(data):
    """Insert just the board."""
    del data['topic_ids']


entityFunctions = {
    'board': {
        'requestor': bitcointalk.requestBoardPage,
        'parser': bitcointalk.parseBoardPage,
        'inserter': _insertBoardPage,
    },
    'member': {
        'requestor': bitcointalk.requestProfile,
        'parser': bitcointalk.parseProfile,
    },
    'topic': {
        'requestor': bitcointalk.requestTopicPage,
        'parser': bitcointalk.parseTopicPage,
    }
}

def _scrape(entity, entityId):
    # global memo
    global entityFunctions
    entityPlural = "{0}s".format(entity)
    html = entityFunctions[entity]['requestor'](entityId)
    datum = entityFunctions[entity]['parser'](html)
    memo[entityPlural].add(entityId)
    return datum


def scrapeBoard(boardId):
    """Scrape information on the specified board."""
    return _scrape('board', boardId)

def scrapeBoardTopics(boardId, pageNum):
    """Scrape topic IDs from a board page. Will not store values."""
    offset = (pageNum - 1) * 40
    html = bitcointalk.requestBoardPage(boardId, offset)
    data = bitcointalk.parseBoardPageTopics(html)
    data = data['topics']
    return data

def scrapeTopicIds(boardId, pageNum):
    """Scrape topic IDs from a board page. Will not store values."""
    offset = (pageNum-1)*40
    html = bitcointalk.requestBoardPage(boardId, offset)
    data = bitcointalk.parseBoardPage(html)
    data = data['topic_ids']
    return data


def scrapeMember(memberId):
    """Scrape the profile of the specified member."""
    return _scrape('member', memberId)


def scrapeMessages(topicId, pageNum):
    """Scrape all messages on the specified topic, page combination."""
    """CAVEAT: Messages are not memoized."""
    offset = (pageNum-1)*20
    html = bitcointalk.requestTopicPage(topicId, offset)
    data = bitcointalk.parseTopicPage(html)
    data = data['messages']
    return data


def scrapeTopic(topicId):
    """Scrape information on the specified topic."""
    return _scrape('topic', topicId)
