""" Module for requesting data from bitcointalk.org and parsing it. """
import codecs
from datetime import datetime, timedelta
from html.parser import HTMLParser
import lxml.html
import requests
import time
import random

baseUrl = "https://bitcointalk.org/index.php"
countRequested = 0
interReqTime = 2
lastReqTime = None


def _request(payloadString):
    """Private method for requesting an arbitrary query string."""
    global countRequested
    global lastReqTime

    if countRequested % 10 == 0 and lastReqTime is not None:
        sleepTime = random.uniform(0.5,2)
        if countRequested % 100 == 0:
           sleepTime = sleepTime * 2
        time.sleep(sleepTime)

    lastReqTime = time.time()

    r = requests.get("{0}?{1}".format(baseUrl, payloadString))

    countRequested += 1
    if r.status_code == requests.codes.ok:
        return r.text
    else:
        raise Exception("Could not process request. Received status code {0}.".format(r.status_code))

def requestBoardPage(boardId, topicOffest=0):
    """Method for requesting a board."""
    return _request("board={0}.{1}".format(boardId, topicOffest))


def requestProfile(memberId):
    """Method for requesting a profile."""
    return _request("action=profile;u={0}".format(memberId))


def requestTopicPage(topicId, messageOffset=0):
    """Method for requesting a topic page."""
    """CAVEAT: Note that a single request will return only 20 messages."""
    return _request("topic={0}.{1}".format(topicId, messageOffset))


def parseBoardPage(html, since=None, until=None):
    """Method for parsing board HTML. Will extract topic IDs."""
    data = {}

    # Extract name
    docRoot = lxml.html.fromstring(html)
    data['name'] = docRoot.cssselect("title")[0].text

    # Parse through board hierarchy
    bodyArea = docRoot.cssselect("#bodyarea")[0]
    linkNodes = bodyArea.cssselect("div > div > div")[0].cssselect("a.nav")
    data['container'] = None
    data['parent'] = None
    for linkNode in linkNodes:
        link = linkNode.attrib["href"]
        linkText = linkNode.text
        linkSuffix = link.split(baseUrl)[1]
        # If this is the top level of the board continue
        if linkSuffix == '':
            continue
        # If this is the container (second to the top level)
        elif linkSuffix[0] == '#':
            data['container'] = linkText
        # If we have something between the board and the container
        elif linkText != data['name']:
            data['parent'] = int(linkSuffix[7:].split(".")[0])
        elif linkText == data['name']:
            data['id'] = int(linkSuffix[7:].split(".")[0])

    # Parse number of pages
    data['num_pages'] = 0
    pageNodes = bodyArea.cssselect(
        "#bodyarea>table td.middletext>a,#bodyarea>table td.middletext>b")
    for pageNode in pageNodes:
        if pageNode.text == " ... " or pageNode.text == "All":
            continue
        elif int(pageNode.text) > data['num_pages']:
            data["num_pages"] = int(pageNode.text)

    # Parse the topic IDs
    topicIds = []
    topics = docRoot.cssselect(
        "#bodyarea>div.tborder>table.bordercolor>tr")

    if since != None:
        since = datetime.strptime(since, '%Y-%m-%d').date()

    data['last_edit_first_topic'] = None

    for topic in topics:
        topicCells = topic.cssselect("td")
        if len(topicCells) != 7:
            continue
        topicLinks = topicCells[2].cssselect("span>a")
        images = topicCells[2].cssselect("img")

        pinned = False
        if len(images) > 0:
            for image in images:
                if image.get("src")[-15:] == "show_sticky.gif":
                    pinned = True

        lastPost = topicCells[6].cssselect("span")[0].text

        lastPost = lastPost.replace("\t", "").replace("\n", "")
        if lastPost == "":
            lastPost = topicCells[6].cssselect("span>b")[0].text
            if lastPost == "Today":
                lastPostDate = datetime.utcnow().date()
        else:
            lastPostDate = datetime.strptime(lastPost, '%B %d, %Y, %I:%M:%S %p').date()

        if data['last_edit_first_topic'] == None and pinned is False:
            data['last_edit_first_topic'] = lastPostDate

        if since == None or since <= lastPostDate:
            if len(topicLinks) > 0:
                linkPayload = topicLinks[0].attrib['href'].replace(
                    baseUrl, '')[1:]
                if linkPayload[0:5] == 'topic':
                    topicIds.append(int(linkPayload[6:-2]))

    data['topic_ids'] = topicIds
    return data


def parseBoardPageTopics(html):
    """Method for parsing board HTML. Will extract topics and their IDs."""
    data = {}

    # Extract name
    docRoot = lxml.html.fromstring(html)
    data['name'] = docRoot.cssselect("title")[0].text

    # Parse through board hierarchy
    bodyArea = docRoot.cssselect("#bodyarea")[0]
    linkNodes = bodyArea.cssselect("div > div > div")[0].cssselect("a.nav")
    data['container'] = None
    data['parent'] = None
    for linkNode in linkNodes:
        link = linkNode.attrib["href"]
        linkText = linkNode.text
        linkSuffix = link.split(baseUrl)[1]
        # If this is the top level of the board continue
        if linkSuffix == '':
            continue
        # If this is the container (second to the top level)
        elif linkSuffix[0] == '#':
            data['container'] = linkText
        # If we have something between the board and the container
        elif linkText != data['name']:
            data['parent'] = int(linkSuffix[7:].split(".")[0])
        elif linkText == data['name']:
            data['id'] = int(linkSuffix[7:].split(".")[0])

    # Parse number of pages
    data['num_pages'] = 0
    pageNodes = bodyArea.cssselect(
        "#bodyarea>table td.middletext>a,#bodyarea>table td.middletext>b")
    for pageNode in pageNodes:
        if pageNode.text == " ... " or pageNode.text == "All":
            continue
        elif int(pageNode.text) > data['num_pages']:
            data["num_pages"] = int(pageNode.text)

    # Parse the topic IDs
    topicDict = []
    topics = docRoot.cssselect(
        "#bodyarea>div.tborder>table.bordercolor>tr")
    for topic in topics:
        topicCells = topic.cssselect("td")
        if len(topicCells) != 7:
            continue
        topicLinks = topicCells[2].cssselect("span>a")
        topicStartedBy = topicCells[3].cssselect("a")
        if len(topicLinks) > 0:
            linkPayload = topicLinks[0].attrib['href'].replace(
                baseUrl, '')[1:]
            if linkPayload[0:5] == 'topic':
                if len(topicStartedBy) > 0:
                    userPayload = topicStartedBy[0].attrib['href'].replace(
                        baseUrl, '')[1:]
                    if userPayload[0:14] == 'action=profile':
                        topicDict.append({"name": topicLinks[0].text, "id": int(linkPayload[6:-2]), "creatorId":int(userPayload[18:-2])})
                else:
                    topicDict.append({"name": topicLinks[0].text, "id": int(linkPayload[6:-2])})

    data['topics'] = topicDict

    return data

def parseProfile(html, todaysDate=datetime.utcnow().date()):
    """Method for parsing profile HTML."""
    data = {}

    docRoot = lxml.html.fromstring(html)

    # Pull the member ID
    pLink = docRoot.cssselect("#bodyarea td.windowbg2 > a")[0].attrib['href']
    data['id'] = int(pLink.split("u=")[1].split(";")[0])

    # Pull associated information
    infoTable = docRoot.cssselect("#bodyarea td.windowbg > table")[0]
    infoRows = infoTable.cssselect("tr")

    labelMapping = {
        "Name: ": "name",
        "Position: ": "position",
        "Posts: ": "posts",
        "Activity:": "activity",
        "Merit:": "merit",
        "Date Registered: ": "date_registered",
        "Last Active: ": "last_active",
        "Email: ": "email",
        "Website: ": "website_name",
        "Bitcoin Address: ": "bitcoin_address",
        "Other contact info: ": "other_contact_info"
    }
    for label, key in labelMapping.items():
        data[key] = None
    data['website_link'] = None
    data['signature'] = None
    for row in infoRows:
        columns = row.cssselect("td")

        if len(columns) != 2:
            signature = row.cssselect("div.signature")
            # if len(signature) == 0:
            #     continue
            # else:
                # sigText = lxml.html.tostring(signature[0])
                # data['signature'] = signature[0].text_content()
        else:

            label = columns[0].text_content()
            if label in labelMapping:
                data[labelMapping[label]] = columns[1].text_content().strip()
            if label == "Website: ":
                linkNode = columns[1].cssselect("a")[0]
                data['website_link'] = linkNode.attrib['href']
            elif label == "Date Registered: " or label == "Last Active: ":
                data[labelMapping[label]] = data[labelMapping[label]].replace(
                    "Today at", todaysDate.strftime("%B %d, %Y,"))
            elif label == "Merit:":
                data[labelMapping[label]] = columns[1].text_content()

    return data


def parseTopicPage(html, since=None, until=None, todaysDate=datetime.utcnow().date()):
    """Method for parsing topic HTML. Will extract messages."""

    if since != None:
        since = datetime.strptime(since, '%Y-%m-%d').date()
    if until != None:
        until = datetime.strptime(until, '%Y-%m-%d').date()
    else:
        until = (datetime.utcnow() + timedelta(days=1)).date()

    data = {}
    h = HTMLParser()
    docRoot = lxml.html.fromstring(html)

    # Parse the topic name
    data['name'] = docRoot.cssselect("title")[0].text

    # Parse through board hierarchy for the containing board ID and topic ID
    bodyArea = docRoot.cssselect("#bodyarea")[0]
    nestedDiv = bodyArea.cssselect("div > div > div")
    if len(nestedDiv) == 0:
        raise Exception("Page does not have valid topic data.")
    linkNodes = nestedDiv[0].cssselect("a.nav")
    for linkNode in linkNodes:
        link = linkNode.attrib["href"]
        linkText = linkNode.text
        linkSuffix = link.split(baseUrl)[1]
        if linkSuffix == '' or linkSuffix[0] == '#':
            continue
        elif linkSuffix[0:6] == "?board":
            data['board'] = int(linkSuffix[7:].split(".")[0])
        elif linkText == data['name']:
            data['id'] = int(linkSuffix[7:].split(".")[0])

    # Parse the total count of pages in the topic
    data['num_pages'] = 0
    pageNodes = bodyArea.cssselect(
        "#bodyarea>table td.middletext>a,#bodyarea>table td.middletext>b")
    for pageNode in pageNodes:
        if pageNode.text == " ... " or pageNode.text == "All":
            continue
        elif int(pageNode.text) > data['num_pages']:
            data["num_pages"] = int(pageNode.text)

    # Parse the read count
    tSubj = docRoot.cssselect("td#top_subject")[0].text.strip()
    data['count_read'] = int(tSubj.split("(Read ")[-1].split(" times)")[0])

    # Parse the messages
    messages = []
    firstPostClass = None
    posts = docRoot.cssselect(
        "form#quickModForm>table.bordercolor>tr")

    data['page_first_message'] = None
    for post in posts:
        if firstPostClass is None:
            firstPostClass = post.attrib["class"]

        if ("class" not in post.attrib or
                post.attrib["class"] != firstPostClass):
            continue
        else:
            m = {}
            m['topic'] = data['id']
            innerPost = post.cssselect("td td.windowbg,td.windowbg2 tr")[0]

            # Parse the member who's made the post
            userInfoPossible = innerPost.cssselect("td.poster_info>b>a")
            if len(userInfoPossible) > 0:
                userInfo = innerPost.cssselect("td.poster_info>b>a")[0]
                userUrlPrefix = "{0}?action=profile;u=".format(baseUrl)
                m['member'] = int(userInfo.attrib["href"].split(
                    userUrlPrefix)[-1])
            # If no links, then we have a guest
            else:
                m['member'] = 0

            # Parse label information about the post
            subj = innerPost.cssselect(
                "td.td_headerandpost>table>tr>td>div.subject>a")[0]
            m['subject'] = subj.text
            m['link'] = subj.attrib['href']
            m['id'] = int(m['link'].split('#msg')[-1])

            # Parse the message post time
            postTime = innerPost.cssselect(
                "td.td_headerandpost>table>tr>td>div.smalltext")[0]
            m['post_time'] = postTime.text_content().strip().replace(
                "Today at", todaysDate.strftime('%B %d, %Y,'))

            # Parse the topic position
            messageNumber = innerPost.cssselect(
                "td.td_headerandpost>table>tr>td>div>a.message_number")[0]
            m['topic_position'] = int(messageNumber.text[1:])

            # Extract the content
            corePost = innerPost.cssselect("div.post")[0]
            # m['content'] = lxml.html.tostring(corePost).strip()[18:-6]
            # m['content_no_html'] = corePost.text_content()

            for child in corePost.iterchildren():
                if (child.tag == "div" and 'class' in child.attrib and
                    (child.attrib['class'] == 'quoteheader' or
                        child.attrib['class'] == 'quote')):
                    corePost.remove(child)

            # Content without quotes and html
            m['content'] = corePost.text_content()

            postTimeDate = datetime.strptime(m['post_time'], '%B %d, %Y, %I:%M:%S %p').date()

            if data['page_first_message'] == None:
                data['page_first_message'] = postTimeDate

            if postTimeDate < until:
                if since == None or since <= postTimeDate:
                    messages.append(m)

    data['messages'] = messages
    return data

