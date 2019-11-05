##
# Custom version of  : https://timber.io/blog/background-tasks-in-python-using-task-queues/
##

from redis import Redis

from simple_redis_queue import SimpleRedisQueue
from serializer import Json, Null

import requests
from bs4 import BeautifulSoup

# Spawn a client connection to redis server. Here Docker
# provieds a link to our local redis server usinf 'redis'
redisClient = Redis()

# Initialize a redis queue instance with name 'bookInfoParser'.
# This name will be used while declaring worker process so that it can
# start processing tasks in it.

json = Json(dumps_kwargs=dict(ensure_ascii=False))
bookInfoParserQueue = SimpleRedisQueue("bookInfoParser", serializer=json)

#################################
###### Methods ##################
#################################

generate_redis_key_for_book = lambda bookURL: "GOODREADS_BOOKS_INFO:" + bookURL


def parse_book_link_for_meta_data(bookLink):
    try:
        htmlString = requests.get(bookLink).content
        bsTree = BeautifulSoup(htmlString, "html.parser")
        title = bsTree.find("h1", attrs={"id": "bookTitle"}).string
        author = bsTree.find("a", attrs={"class": "authorName"}).span.string
        rating = bsTree.find("span", attrs={"itemprop": "ratingValue"}).string
        descr_span = bsTree.find("div", attrs={"id": "description"}).find("span", attrs={"style": "display:none"})
        if descr_span is not None:
            description = "".join( descr_span.stripped_strings)
        else:
            description = ""
        return dict(
            title=title.strip() if title else "",
            author=author.strip() if author else "",
            rating=float(rating.strip() if rating else 0),
            description=description,
        )
    except Exception :
        print(bookLink)
        return

def parse_and_persist_book_info(bookUrl):
    redisKey = generate_redis_key_for_book(bookUrl)
    bookInfo = parse_book_link_for_meta_data(bookUrl)
    redisClient.set(redisKey, json.dumps(bookInfo))


def parse_goodreads_urls(urls):
    if (isinstance(urls,list) and len(urls)):
        bookLinksArray = [x for x in list(set(urls)) if x.startswith('https://www.goodreads.com/book/show/')]
        if (len(bookLinksArray)):
            for bookUrl in bookLinksArray:
                bookInfoParserQueue.put(bookUrl)
                # bookInfoParserQueue.enqueue_call(func=parse_and_persist_book_info,args=(bookUrl,),job_id=bookUrl)
            return "%d books are scheduled for info parsing."%(len(bookLinksArray))
    return "Only array of goodreads book links is accepted.",400


# def get_goodreads_book_info():
#     bookURL = request.args.get('url', None)
#     if (bookURL and bookURL.startswith('https://www.goodreads.com/book/show/')):
#         redisKey = generate_redis_key_for_book(bookURL)
#         cachedValue = redisClient.get(redisKey)
#         if cachedValue:
#             return jsonify(pickle.loads(cachedValue))
#         return "No meta info found for this book."
#     return "'url' query parameter is required. It must be a valid goodreads book URL.",400


if __name__ == '__main__':
    urls = [
        "https://www.goodreads.com/book/show/42975172-the-testaments",
        "https://www.goodreads.com/book/show/41723456-the-overdue-life-of-amy-byler",
        "https://www.goodreads.com/book/show/41057294-normal-people",
        "https://www.goodreads.com/book/show/39863515-an-anonymous-girl",
        "https://www.goodreads.com/book/show/40697540-run-away",
        "https://www.goodreads.com/book/show/38819868-my-sister-the-serial-killer",
        "https://www.goodreads.com/book/show/43982054-the-water-dancer",
        "https://www.goodreads.com/book/show/2784926-the-beautiful-struggle",
        "https://www.goodreads.com/book/show/36529.Narrative_of_the_Life_of_Frederick_Douglass",
        "https://www.goodreads.com/book/show/6792458-the-new-jim-crow",
        "https://www.goodreads.com/book/show/16280._Why_Are_All_The_Black_Kids_Sitting_Together_in_the_Cafeteria_",
        "https://www.goodreads.com/book/show/13214.I_Know_Why_the_Caged_Bird_Sings",
        "https://www.goodreads.com/book/show/325779.Next_of_Kin",
        "https://www.goodreads.com/book/show/39943621-fire-blood",
        "https://www.goodreads.com/book/show/42036965-one-word-kill",
        "https://www.goodreads.com/book/show/41940388-the-test",
        "https://www.goodreads.com/book/show/40574441-everything-is-f-cked",
        "https://www.goodreads.com/book/show/43848929-talking-to-strangers",
        "https://www.goodreads.com/book/show/43497893-prognosis",
        "https://www.goodreads.com/book/show/40696923-blueprint",
        "https://www.goodreads.com/book/show/40180060-mama-s-last-hug",
        "https://www.goodreads.com/book/show/43852758-how-to",
        "https://www.goodreads.com/book/show/40645634-notes-from-a-young-black-chef",
        "https://www.goodreads.com/book/show/41746324-the-tradition",
        "https://www.goodreads.com/book/show/43453732-when-you-ask-me-where-i-m-going",
        "https://www.goodreads.com/book/show/42785750-sulwe"
    ]
    parse_goodreads_urls(urls)
    q2 = SimpleRedisQueue("bookInfoParser", serializer=json)
    for url in q2.consume(limit=3):
        # print(url)
        parse_and_persist_book_info(url)
    