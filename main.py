import glob
import logging
import os
import zipfile
from datetime import datetime
from typing import Dict

import yaml
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from book_tools.format.fb2sax import FB2sax
from book_tools.format.util import strip_symbols
from models.models import (
    OpdsCatalogBook,
    OpdsCatalogCatalog,
    OpdsCatalogAuthor,
    OpdsCatalogSery,
    OpdsCatalogBauthor,
    OpdsCatalogBsery,
)

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def load_config(config_path: str) -> Dict:
    """Load configuration from YAML file."""
    try:
        with open(config_path, "r") as f:
            return yaml.safe_load(f)
    except Exception as e:
        logger.error(f"Failed to load config: {e}")
        raise


def get_or_create(session, model, **kwargs):
    """Get or create a database model instance."""
    instance = session.query(model).filter_by(**kwargs).first()
    if not instance:
        instance = model(**kwargs)
        session.add(instance)
        session.flush()
    return instance


def process_author_name(author_name: str) -> str:
    """Process author name to standardize format."""
    if "," not in author_name:
        names = author_name.split()
        if names:
            author_name = f"{names[-1]} {' '.join(names[:-1])}"
    return author_name


def save_cover_image(cover_data: bytes, filename: str) -> bool:
    """Save cover image to disk."""
    try:
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        with open(filename, "wb") as cover:
            cover.write(cover_data)
        return True
    except OSError as e:
        logger.error(f"Failed to save cover {filename}: {e}")
        return False


class BookProcessor:
    """Class to handle book archive processing and database insertion."""

    def __init__(self, config_path: str = "config.yaml"):
        self.config = load_config(config_path)
        self.engine = create_engine(self.config["db_url"])
        self.Session = sessionmaker(bind=self.engine, autocommit=True)

    def clean_text(self, text) -> str:
        """Clean text by decoding if bytes and stripping symbols."""
        if isinstance(text, bytes):
            text = text.decode("utf-8", errors="ignore")
        return (text or "").strip(strip_symbols)

    def get_unscanned_archives(self, session) -> list:
        """Get list of unscanned archive paths."""
        books_path = self.config["path_to_archives"]
        archives_list = [os.path.basename(a) for a in glob.glob(f"{books_path}/*.zip")]
        db_archives = {a.cat_name for a in session.query(OpdsCatalogCatalog).all()}
        unscanned_names = [a for a in archives_list if a not in db_archives]
        return [os.path.join(books_path, name) for name in unscanned_names]

    def scan_archive(self, archive_path: str, session) -> None:
        """Scan and process a single archive."""
        archive_name = os.path.basename(archive_path)
        logger.info(f"Processing archive: {archive_name}")

        with zipfile.ZipFile(archive_path) as scan_it:
            for file_name in (f for f in scan_it.namelist() if f.lower().endswith(".fb2")):
                try:
                    with scan_it.open(file_name) as book:
                        self.process_book(file_name, archive_name, book, session)
                except Exception as e:
                    logger.error(f"Error processing file {file_name}: {e}")
                    continue

        session.add(OpdsCatalogCatalog(cat_name=archive_name, is_scanned=True))
        session.flush()

    def process_book(self, file_name: str, archive_name: str, book, session) -> None:
        """Process a single book file from the archive."""
        try:
            zipped_book = FB2sax(book, file_name)
        except Exception as e:
            logger.error(f"Failed to parse book {file_name}: {e}")
            return

        # Prepare book metadata
        annotation = self.clean_text(zipped_book.description)[:1000]
        title = self.clean_text(zipped_book.title) or file_name

        book_object = OpdsCatalogBook(
            filename=file_name,
            path=archive_name,
            format="fb2",
            registerdate=datetime.now(),
            docdate=self.clean_text(zipped_book.docdate),
            lang=self.clean_text(zipped_book.language_code),
            title=title,
            annotation=annotation,
        )
        session.add(book_object)
        session.flush()

        self.process_cover(zipped_book, book_object, archive_name, file_name)
        self.process_authors(zipped_book, book_object, session)
        self.process_series(zipped_book, book_object, session)
        session.flush()

    def process_cover(self, zipped_book, book_object, archive_name: str, file_name: str) -> None:
        """Extract and save book cover if available."""
        cover_extr = zipped_book.extract_cover_memory()
        if cover_extr:
            cover_dir = f"{self.config['path_to_covers']}{archive_name.replace('.', '-')}/"
            cover_filename = f"{cover_dir}{file_name.replace('.', '-')}.jpg"
            book_object.cover = save_cover_image(cover_extr, cover_filename)

    def process_authors(self, zipped_book, book_object, session) -> None:
        """Process and add book authors."""
        authors = zipped_book.authors or [{}]
        for author in authors:
            raw_name = self.clean_text(author.get("name", ""))
            author_name = process_author_name(raw_name) if raw_name else "Author Unknown"
            author_obj = get_or_create(session, OpdsCatalogAuthor, full_name=author_name)
            session.add(OpdsCatalogBauthor(author_id=author_obj.id, book_id=book_object.id))

    def process_series(self, zipped_book, book_object, session) -> None:
        """Process and add book series info if available."""
        if zipped_book.series_info:
            series_title = self.clean_text(zipped_book.series_info.get("title", ""))
            if series_title:
                series = get_or_create(session, OpdsCatalogSery, ser=series_title)
                index_str = zipped_book.series_info.get("index", "0")
                ser_no = int(index_str) if index_str.isdigit() else 0
                session.add(OpdsCatalogBsery(ser_no=ser_no, ser_id=series.id, book_id=book_object.id))

    def process(self) -> None:
        """Main process to scan and handle unscanned archives."""
        with self.Session() as session:
            unscanned_archives = self.get_unscanned_archives(session)
            for archive_path in unscanned_archives:
                self.scan_archive(archive_path, session)


if __name__ == "__main__":
    processor = BookProcessor()
    processor.process()