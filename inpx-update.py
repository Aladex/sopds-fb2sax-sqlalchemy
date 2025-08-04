import zipfile
import logging
import os
from typing import Dict, List, Set, Tuple
from collections import defaultdict

import yaml
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

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
    """Process author name to standardize format, dropping patronymic."""
    parts = [part.strip() for part in author_name.split(',') if part.strip()]
    if len(parts) >= 2:
        return ' '.join(parts[:2])
    return ' '.join(parts)


class BookProcessor:
    """Class to handle book metadata comparison between INPX and DB."""

    def __init__(self, config_path: str = "config.yaml"):
        self.config = load_config(config_path)
        self.engine = create_engine(self.config["db_url"])
        self.Session = sessionmaker(bind=self.engine)
        self.diff_path = self.config.get("diff_path", "inpx_diff.txt")
        self.inpx_path = self.config.get("inpx_path", "librusec_local_fb2.inpx")
        self.default_fields = ["AUTHOR", "GENRE", "TITLE", "SERIES", "SERNO", "FILE", "SIZE", "LIBID", "DEL", "EXT", "DATE"]
        self.batch_size = 1000

    def clean_text(self, text: str) -> str:
        """Clean text by stripping symbols."""
        return (text or "").strip(strip_symbols).strip()

    def get_structure(self, zf: zipfile.ZipFile) -> List[str]:
        """Get field structure from structure.info or default."""
        logger.info("Fetching structure from INPX.")
        if "structure.info" in zf.namelist():
            with zf.open("structure.info") as f:
                structure = f.read().decode("utf-8").strip().rstrip(";").split(";")
                logger.info(f"Structure found: {structure}")
                return structure
        logger.info(f"No structure.info, using default: {self.default_fields}")
        return self.default_fields

    def collect_inpx_data(self) -> Dict[Tuple[str, str], Dict]:
        """Collect data from INPX file into a dict keyed by (archive_name, filename)."""
        logger.info("Starting collection from INPX.")
        inpx_data = {}
        with zipfile.ZipFile(self.inpx_path) as zf:
            fields = self.get_structure(zf)
            inp_files = [n for n in zf.namelist() if n.endswith(".inp")]
            logger.info(f"Found {len(inp_files)} .inp files to process.")
            for inp_name in inp_files:
                archive_name = inp_name.replace(".inp", ".zip")
                logger.info(f"Processing .inp file: {inp_name} for archive: {archive_name}")
                try:
                    with zf.open(inp_name) as f:
                        line_count = 0
                        for line in f:
                            line_str = line.decode("utf-8", errors="ignore").strip()
                            if not line_str:
                                continue
                            parts = line_str.split(chr(4))
                            if len(parts) < len(fields):
                                logger.warning(f"Skipping invalid line in {inp_name}: too few parts.")
                                continue
                            entry = dict(zip(fields, parts))
                            if entry.get("DEL", "0") == "1":
                                logger.debug(f"Skipping deleted entry: {entry['FILE']}")
                                continue
                            filename = f"{entry['FILE']}.{entry['EXT']}"
                            key = (archive_name, filename)
                            authors_str = self.clean_text(entry["AUTHOR"]).rstrip(":")
                            authors = sorted([process_author_name(self.clean_text(a)) for a in authors_str.split(":") if a] or ["Author Unknown"])
                            series = self.clean_text(entry["SERIES"])
                            ser_no_str = entry["SERNO"]
                            ser_no = int(ser_no_str) if ser_no_str.isdigit() else 0
                            inpx_data[key] = {
                                "title": self.clean_text(entry["TITLE"]),
                                "docdate": entry.get("DATE", ""),
                                "lang": "ru",  # Assumed
                                "authors": authors,
                                "series": series,
                                "ser_no": ser_no,
                            }
                            line_count += 1
                        logger.info(f"Processed {line_count} entries from {inp_name}.")
                except Exception as e:
                    logger.error(f"Error collecting from {inp_name}: {e}")
                    continue
        logger.info(f"Collected {len(inpx_data)} entries from INPX.")
        return inpx_data

    def compare_inpx_with_db(self) -> None:
        """Compare INPX data with DB, log mismatches, and update DB if mismatched."""
        logger.info("Starting comparison between INPX and DB.")
        inpx_data = self.collect_inpx_data()
        inpx_keys = set(inpx_data.keys())
        missing_in_db_count = 0
        missing_in_inpx_count = 0
        match_count = 0
        mismatch_count = 0
        common_count = 0

        with open(self.diff_path, "w", encoding="utf-8") as diff_file:
            diff_file.write("=== Books in INPX but missing in DB ===\n")
            diff_file.write("=== Field comparisons for common books ===\n\n")

            session = self.Session()
            try:
                for i, (key, inpx_entry) in enumerate(inpx_data.items(), 1):
                    if i % 10000 == 0:
                        logger.info(f"Processed {i} INPX entries...")

                    logger.debug(f"Querying DB for: {key}")
                    book = session.query(OpdsCatalogBook).filter_by(path=key[0], filename=key[1]).first()
                    if book:
                        common_count += 1
                        authors_query = session.query(OpdsCatalogAuthor.full_name).join(OpdsCatalogBauthor).filter(OpdsCatalogBauthor.book_id == book.id).all()
                        authors = sorted([self.clean_text(a[0]) for a in authors_query])
                        series_info = session.query(OpdsCatalogSery.ser, OpdsCatalogBsery.ser_no).join(OpdsCatalogBsery).filter(OpdsCatalogBsery.book_id == book.id).first()
                        series = self.clean_text(series_info[0]) if series_info else ""
                        ser_no = series_info[1] if series_info else 0
                        db_entry = {
                            "title": self.clean_text(book.title),
                            "docdate": self.clean_text(book.docdate),
                            "lang": self.clean_text(book.lang),
                            "authors": authors,
                            "series": series,
                            "ser_no": ser_no,
                        }
                        field_mismatches = []
                        for field in ["title", "docdate", "lang", "authors", "series", "ser_no"]:
                            if inpx_entry[field] != db_entry[field]:
                                field_mismatches.append((field, db_entry[field], inpx_entry[field]))
                        diff_file.write(f"For {key[0]} / {key[1]}:\n")
                        if field_mismatches:
                            mismatch_count += 1
                            diff_file.write("Not matched\n")
                            for field, db_val, inpx_val in field_mismatches:
                                diff_file.write(f"  {field.capitalize()}: DB={db_val!r} -> INPX={inpx_val!r}\n")
                            # Update DB with INPX data
                            logger.info(f"Updating book {key} with INPX data.")
                            book.title = inpx_entry["title"]
                            book.docdate = inpx_entry["docdate"]
                            book.lang = inpx_entry["lang"]
                            # Update authors
                            session.query(OpdsCatalogBauthor).filter_by(book_id=book.id).delete()
                            for author_name in inpx_entry["authors"]:
                                author_obj = get_or_create(session, OpdsCatalogAuthor, full_name=author_name)
                                session.add(OpdsCatalogBauthor(author_id=author_obj.id, book_id=book.id))
                            # Update series
                            session.query(OpdsCatalogBsery).filter_by(book_id=book.id).delete()
                            if inpx_entry["series"]:
                                series_obj = get_or_create(session, OpdsCatalogSery, ser=inpx_entry["series"])
                                session.add(OpdsCatalogBsery(ser_no=inpx_entry["ser_no"], ser_id=series_obj.id, book_id=book.id))
                            session.flush()
                            session.commit()
                            diff_file.write("Updated in DB\n")
                        else:
                            match_count += 1
                            diff_file.write("Matched\n")
                        diff_file.write("\n")
                    else:
                        missing_in_db_count += 1
                        diff_file.write(f"Missing in DB: {key[0]} / {key[1]}\n")

                diff_file.write("\n=== Books in DB but missing in INPX ===\n")
                offset = 0
                while True:
                    logger.info(f"Querying DB batch for missing in INPX: offset {offset}, limit {self.batch_size}")
                    books = session.query(OpdsCatalogBook).limit(self.batch_size).offset(offset).all()
                    if not books:
                        logger.info("No more books in DB for missing check.")
                        break
                    for book in books:
                        key = (book.path, book.filename)
                        if key not in inpx_keys:
                            missing_in_inpx_count += 1
                            diff_file.write(f"Missing in INPX: {key[0]} / {key[1]}\n")
                    offset += self.batch_size

                diff_file.write(f"\nTotal common books: {common_count}\n")
                diff_file.write(f"Matched: {match_count}\n")
                diff_file.write(f"Mismatched: {mismatch_count}\n")
                diff_file.write(f"Missing in DB: {missing_in_db_count}\n")
                diff_file.write(f"Missing in INPX: {missing_in_inpx_count}\n")

            finally:
                session.close()

        logger.info(f"Comparison complete. Results written to {self.diff_path}")
        logger.info(f"Missing in DB: {missing_in_db_count}")
        logger.info(f"Missing in INPX: {missing_in_inpx_count}")
        logger.info(f"Common books: {common_count}")
        logger.info(f"Matched: {match_count}")
        logger.info(f"Mismatched: {mismatch_count}")


if __name__ == "__main__":
    processor = BookProcessor()
    processor.compare_inpx_with_db()