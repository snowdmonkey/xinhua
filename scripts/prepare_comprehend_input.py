import csv
from pathlib import Path

from xinhua.data import ColumnHeader

OUTPUT_FOLDER = Path("data/comprehend_input")


with open("data/1000.csv") as f:
    reader = csv.DictReader(f, fieldnames=[x.value for x in ColumnHeader])
    next(reader)
    for i, row in enumerate(reader):
        with open(OUTPUT_FOLDER/f"{row[ColumnHeader.BOOK_ID.value]}.txt", "w") as f_book:
            f_book.write(row[ColumnHeader.SUMMARY.value])


