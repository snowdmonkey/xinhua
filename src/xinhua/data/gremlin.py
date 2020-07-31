import csv
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import List, Dict, Optional, Tuple
import re

from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection
from gremlin_python.process.anonymous_traversal import traversal
from gremlin_python.process.graph_traversal import GraphTraversalSource
from gremlin_python.process.traversal import Cardinality
from gremlin_python.structure.graph import Vertex, Edge

from . import ColumnHeader


logger = logging.getLogger(__name__)


@dataclass
class Node:

    @property
    def label(self) -> str:
        return self.__class__.__name__

    def put(self, g: GraphTraversalSource) -> Vertex:
        """add the node to gremlin server and return the vertex created; or return the vertex directly if the node
        already exists. Existence of a node is decided by node id if this property exists, otherwise by name"""
        try:
            v = g.V().has(self.label, self.identifier, getattr(self, self.identifier)).next()
        except StopIteration:
            return self._add(g)
        else:
            return v

    def _add(self, g: GraphTraversalSource) -> Vertex:
        t = g.addV(self.label)
        for k, v in self.__dict__.items():
            if v is not None:
                t = t.property(Cardinality.single, k, v)
        return t.next()

    @property
    def identifier(self):
        return "id" if "id" in self.__dict__ else "name"

    def connect(self, dst: "Node", g: GraphTraversalSource, relation_label: str) -> Edge:
        """create an edge from self node to dst node if it does not exist.
        :return: created edge or existed edge
        """
        src_v = self.put(g)
        dst_v = dst.put(g)
        try:
            edge = g.V(src_v).outE(relation_label).outV().has(dst.label, dst.identifier, getattr(dst, dst.identifier))\
                .next()
        except StopIteration:
            edge = g.addE(relation_label).from_(src_v).to(dst_v).next()
            return edge
        else:
            return edge


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
    conn = DriverRemoteConnection(
        "wss://liuxuefe-xinhua.cluster-cnmovwdys94f.us-east-1.neptune.amazonaws.com:8182/gremlin", "g")
    g = traversal().withRemote(conn)

    extractor = EntityExtractor()

    with open("data/1000.csv") as f:
        reader = csv.DictReader(f, fieldnames=[x.value for x in ColumnHeader])
        next(reader)
        for i, row in enumerate(reader):

            book = extractor.extract_book(row)
            book.put(g)
            authors = extractor.extract_authors(row)
            for relation, persons in authors.items():
                for person in persons:
                    person.connect(book, g, relation)

            book_series = extractor.extract_book_series(row)
            if book_series is not None:
                book.connect(book_series, g, "IS_IN_SERIES")

            publisher = extractor.extract_publisher(row)
            publisher.connect(book, g, "PUBLISH")

            topics = extractor.extract_topics(row)
            for topic in topics:
                book.connect(topic, g, "HAS_TOPIC")

            cn_category = extractor.extract_cn_category(row)
            if cn_category is not None:
                book.connect(cn_category, g, "IS_IN_CN_CATEGORY")

            category = extractor.extract_category(row)
            if category is not None:
                book.connect(category, g, "IS_IN_CATEGORY")

            if i % 100 == 0:
                logger.info(f"processed {i} rows")

    conn.close()


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    main()
