""" Module for loading parsed data from bitcointalk into PostgreSQL. """
from . import bitcointalk

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

def _scrape(entity, entityId, since=None, until=None):
    # global memo
    global entityFunctions
    entityPlural = "{0}s".format(entity)
    html = entityFunctions[entity]['requestor'](entityId)

    datum = entityFunctions[entity]['parser'](html, since, until)

    memo[entityPlural].add(entityId)
    return datum


def scrapeBoard(boardId, since=None, until=None):
    """Scrape information on the specified board."""
    return _scrape('board', boardId, since, until)

def scrapeBoardTopics(boardId, pageNum):
    """Scrape topic IDs from a board page. Will not store values."""
    offset = (pageNum - 1) * 40
    html = bitcointalk.requestBoardPage(boardId, offset)
    data = bitcointalk.parseBoardPageTopics(html)
    data = data['topics']
    return data

def scrapeTopicIds(boardId, pageNum, since=None, until=None):
    """Scrape topic IDs from a board page. Will not store values."""
    offset = (pageNum-1)*40
    try:
        html = bitcointalk.requestBoardPage(boardId, offset)
        data = bitcointalk.parseBoardPage(html, since, until)
        # data = data['topic_ids']
        return data
    except Exception as e:
        raise Exception(e)



def scrapeMember(memberId):
    """Scrape the profile of the specified member."""
    return _scrape('member', memberId)


def scrapeMessages(topicId, pageNum, since=None, until=None):
    """Scrape all messages on the specified topic, page combination."""
    """CAVEAT: Messages are not memoized."""
    try:
        offset = (pageNum-1)*20
        html = bitcointalk.requestTopicPage(topicId, offset)
        data = bitcointalk.parseTopicPage(html, since, until)
        # data = data['messages']
        return data
    except Exception as e:
        raise Exception(e)


def scrapeTopic(topicId, since=None, until=None):
    """Scrape information on the specified topic."""
    try:
        return _scrape('topic', topicId, since, until)
    except Exception as e:
        raise Exception(e)