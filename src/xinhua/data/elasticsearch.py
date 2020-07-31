import csv
import logging

from elasticsearch import Elasticsearch

from . import ColumnHeader


logger = logging.getLogger(__name__)


def create_index():
    es = Elasticsearch()

    es.indices.create(index="book")
    es.indices.put_mapping({
        "properties": {
            "name": {"type": "text", "analyzer": "ik_max_word", "search_analyzer": "ik_smart"},
            "author": {"type": "text", "analyzer": "ik_max_word", "search_analyzer": "ik_smart"},
            "topic": {"type": "text", "analyzer": "ik_max_word", "search_analyzer": "ik_smart"},
            "summary": {"type": "text", "analyzer": "ik_max_word", "search_analyzer": "ik_smart"}
        }
    }, "book")

    es.close()


def load_books_information_to_elastic_search():
    es = Elasticsearch()

    with open("data/1000.csv") as f:
        reader = csv.DictReader(f, fieldnames=[x.value for x in ColumnHeader])
        next(reader)
        for i, row in enumerate(reader):
            es.create("book", row[ColumnHeader.BOOK_ID.value], body={
                "name": row[ColumnHeader.BOOK_NAME_STR.value],
                "author": row[ColumnHeader.AUTHOR_STR.value],
                "topic": row[ColumnHeader.TOPIC_STR.value],
                "summary": row[ColumnHeader.SUMMARY.value]
            })

            if i % 100 == 0:
                logger.info(f"processed {i} rows")
    es.close()


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    load_books_information_to_elastic_search()
