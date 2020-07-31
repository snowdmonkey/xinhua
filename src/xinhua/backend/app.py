from pathlib import Path
from typing import Optional, List

from fastapi import FastAPI
from pydantic import BaseModel
import elasticsearch
import numpy as np
import faiss


app = FastAPI()

es = elasticsearch.Elasticsearch()


class Book(BaseModel):
    id: str
    name: str
    author: str
    topic: str


class RelevantBookExtractor:

    def __init__(self, node_ids: List[str], node_embeddings: np.ndarray):
        self._node_ids = node_ids
        self._node_id_to_index = {node_id: i for i, node_id in enumerate(node_ids)}
        self._node_embeddings = node_embeddings
        self._embedding_dim = node_embeddings.shape[1]

        self._index = faiss.IndexFlatL2(self._embedding_dim)
        self._index.add(node_embeddings)

    def get_nearest_k(self, book_id: str, k: int):
        book_id = f"Book/{book_id}"
        book_index = self._node_id_to_index[book_id]
        _, nearest_book_indices = self._index.search(
            self._node_embeddings[book_index].reshape(1, self._embedding_dim), k+1)
        nearest_book_indices = nearest_book_indices[0, 1:]
        nearest_book_ids = [self._node_ids[x] for x in nearest_book_indices]
        print(nearest_book_ids)
        nearest_book_ids = [x.replace("Book/", "") for x in nearest_book_ids if x.startswith("Book/")]
        return nearest_book_ids


def get_book_ids(entities_path: Path):
    with entities_path.open("r") as f:
        lines = f.readlines()
    return [line.split("\t")[1].strip() for line in lines]


def get_book_embeddings(embedding_path: Path):
    return np.load(str(embedding_path))


relevant_book_extractor = RelevantBookExtractor(
    node_ids=get_book_ids(Path("data/entities.tsv")),
    node_embeddings=get_book_embeddings(Path("data/ckpts/DistMult_book_0/book_DistMult_entity.npy"))
)


@app.get("/books")
def search_book(q: str, max_hit: int = 1, max_relevant: int = 2):
    res = es.search(index="book", body={"query": {"match": {"name": q}}})
    books_hit = list()
    for hit in res["hits"]["hits"][:max_hit]:
        books_hit.append(Book(
            id=hit["_id"],
            name=hit["_source"]["name"],
            author=hit["_source"]["author"],
            topic=hit["_source"]["topic"]
        ))
    books_relevant = list()
    if len(books_hit) > 0:
        relevant_book_ids = relevant_book_extractor.get_nearest_k(books_hit[0].id, 500)[:max_relevant]
        for book_id in relevant_book_ids:
            res = es.get("book", book_id)
            books_relevant.append(Book(
                id=res["_id"],
                name=res["_source"]["name"],
                author=res["_source"]["author"],
                topic=res["_source"]["topic"]
            ))

    return {"books_hit": books_hit, "books_relevant": books_relevant}
