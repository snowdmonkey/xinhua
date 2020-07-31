import csv
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import List, Dict, Optional, Tuple
import re

from neo4j import Session, Transaction, Driver, GraphDatabase

from . import ColumnHeader


logger = logging.getLogger(__name__)


@dataclass
class Node:

    @property
    def label(self) -> str:
        return self.__class__.__name__

    def _created(self, tx) -> bool:
        try:
            q = f"MATCH (a:{self.label}) " \
                f"WHERE a.{self.identifier}=$identifier_value " \
                f"RETURN a.{self.identifier} LIMIT 1"
            r = tx.run(q, identifier_value=self.identifier_value)
        except Exception as e:
            raise e
        return len(list(r)) > 0

    def _create_self(self, tx: Transaction):
        cql = f"CREATE (n:{self.label}) "
        for k, v in self.__dict__.items():
            if v is not None:
                cql += fr"SET n.{k}=${k} "
        tx.run(cql, **self.__dict__)

    def put(self, driver: Driver):
        """add the node to neo4j server and return the vertex created; or return the vertex directly if the node
        already exists. Existence of a node is decided by node id if this property exists, otherwise by name

        :return identifier, identifier value
        """
        with driver.session() as session:
            if not session.read_transaction(self._created):
                session.write_transaction(self._create_self)

    @property
    def identifier(self) -> str:
        return "id" if "id" in self.__dict__ else "name"

    @property
    def identifier_value(self) -> str:
        return getattr(self, self.identifier)

    def _connect_tx(self, tx: Transaction, dst: "Node", relation_label: str):
        q = f"MATCH (a:{self.label}), (b:{dst.label}) " \
            fr"WHERE a.{self.identifier}= $src_identifier_value and b.{dst.identifier}=$dst_identifier_value " \
            f"MERGE (a)-[r:{relation_label}]->(b)"
        tx.run(q, src_identifier_value=self.identifier_value, dst_identifier_value=dst.identifier_value)

    def connect(self, dst: "Node", driver: Driver, relation_label: str):
        """create an edge from self node to dst node if it does not exist.
        :return: created edge or existed edge
        """
        self.put(driver)
        dst.put(driver)
        with driver.session() as session:
            session.write_transaction(self._connect_tx, dst, relation_label)


@dataclass
class Person(Node):
    name: str
    country: str = None


@dataclass
class BookSeries(Node):
    name: str


@dataclass
class Publisher(Node):
    id: str
    name: str


@dataclass
class Topic(Node):
    name: str


@dataclass
class CNCategory(Node):
    id: str


@dataclass
class Category(Node):
    id: str
    name: str
    level: int


@dataclass
class Book(Node):
    id: str
    name: str


class EntityExtractor:
    def extract(self, row: Dict[str, str]):
        pass

    def extract_book(self, row: Dict[str, str]) -> Book:
        # assert row[ColumnHeader.BOOK_ID.value] == row[ColumnHeader.BOOK_ID2.value]
        book_id = row[ColumnHeader.BOOK_ID.value]
        book_name = row[ColumnHeader.BOOK_NAME_STR.value].split("/")[0]
        return Book(id=book_id, name=book_name)

    def extract_book_series(self, row: Dict[str, str]) -> Optional[BookSeries]:
        words = row[ColumnHeader.BOOK_NAME_STR.value].split("/")
        if len(words) == 1:
            return None
        else:
            return BookSeries(name="/".join(words[1:]))

    def extract_authors(self, row: Dict[str, str]) -> Dict[str, List[Person]]:
        """extract authors and editors of a book

        :return: a dict, key is relation between person and book, e.g., write, 责编， 主编。。。
        """
        d = dict()
        author_str = row[ColumnHeader.AUTHOR_STR.value]
        words = author_str.split("|")
        for word in words:
            m = re.match(r"(?P<relation>.+):(?P<person_str>.+)", word)
            if m is not None:
                d[m.group("relation")] = self._extract_persons(m.group("person_str"))
            else:
                d["WRITE"] = self._extract_persons(word)
        return d

    def _extract_persons(self, s: str) -> List[Person]:
        """extract persons from a string with pattern (country)name//(country)name//...
        e.g. (南朝梁)刘孝标//龚斌,
        """
        words = s.split("//")
        persons = list()
        for word in words:
            m = re.match(r"\((?P<country>.+)\)(?P<name>.+)", word)
            if m is not None:
                persons.append(Person(name=m.group("name"), country=m.group("country")))
            else:
                persons.append(Person(name=word))
        return persons

    def extract_publisher(self, row: Dict[str, str]) -> Publisher:
        return Publisher(id=row[ColumnHeader.PUBLISHER_ID.value], name=row[ColumnHeader.PUBLISHER_NAME.value])

    def extract_topics(self, row: Dict[str, str]) -> List[Topic]:
        topic_str = row[ColumnHeader.TOPIC_STR.value]
        if topic_str == "":
            return list()
        topic_names = set()
        for word in topic_str.split("//"):
            m = re.match(r"[0-9]?(?P<topic_name>[^0-9].*)", word)
            if m is None:
                logger.warning(f"cannot process {word}")
                continue
            else:
                topic_names.add(m.group("topic_name"))

        return [Topic(name=x) for x in topic_names]

    def extract_cn_category(self, row: Dict[str, str]) -> Optional[CNCategory]:
        cn_category_id = row[ColumnHeader.CN_CATEGORY.value]
        if cn_category_id == "":
            return None
        else:
            return CNCategory(id=row[ColumnHeader.CN_CATEGORY.value])

    def extract_category(self, row: Dict[str, str]) -> Category:
        return Category(
            id=row[ColumnHeader.CATEGORY3_ID.value],
            name=row[ColumnHeader.CATEGORY3_NAME.value],
            level=3
        )


def main():
    driver = GraphDatabase.driver("neo4j://localhost:7687", auth=("neo4j", "2much4ME"))

    extractor = EntityExtractor()

    with open("data/1000.csv") as f:
        reader = csv.DictReader(f, fieldnames=[x.value for x in ColumnHeader])
        next(reader)
        for i, row in enumerate(reader):

            book = extractor.extract_book(row)
            book.put(driver)
            authors = extractor.extract_authors(row)
            for relation, persons in authors.items():
                for person in persons:
                    person.connect(book, driver, relation)

            book_series = extractor.extract_book_series(row)
            if book_series is not None:
                book.connect(book_series, driver, "IS_IN_SERIES")

            publisher = extractor.extract_publisher(row)
            publisher.connect(book, driver, "PUBLISH")

            topics = extractor.extract_topics(row)
            for topic in topics:
                book.connect(topic, driver, "HAS_TOPIC")

            cn_category = extractor.extract_cn_category(row)
            if cn_category is not None:
                book.connect(cn_category, driver, "IS_IN_CN_CATEGORY")

            category = extractor.extract_category(row)
            if category is not None:
                book.connect(category, driver, "IS_IN_CATEGORY")

            if i % 100 == 0:
                logger.info(f"processed {i} rows")

    driver.close()


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    main()
