import glob
import logging
import os
import zipfile
from datetime import datetime
from typing import Dict

import yaml
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from book_tools.format.fb2sax import FB2Sax
from book_tools.format.util import strip_symbols
from models.models import (
    OpdsCatalogBook,
    OpdsCatalogCatalog,
    OpdsCatalogAuthor,
    OpdsCatalogSery,
    OpdsCatalogBauthor,
    OpdsCatalogBsery,
)

from openai import OpenAI
from langdetect import detect as langdetect_detect
import langid

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
        session.flush()  # Flush to get ID if needed, but within transaction
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
    """Class to handle book archive processing, database insertion, and advanced language detection."""

    def __init__(self, config_path: str = "config.yaml"):
        self.config = load_config(config_path)
        self.engine = create_engine(self.config["db_url"])
        self.Session = sessionmaker(bind=self.engine, future=True)
        self.lang_cache = {}
        self.equivalent_pairs = {
            ("ru", "uk"), ("uk", "ru"),
            ("ru", "bg"), ("bg", "ru"),
            ("bg", "mk"), ("mk", "bg"),
        }
        # Initialize OpenAI if API key is provided
        self.openai = None
        if "openai_api_key" in self.config and self.config["openai_api_key"]:
            self.openai = OpenAI(api_key=self.config["openai_api_key"])
        else:
            logger.warning("OpenAI API key not found in config. Language detection fallback to OpenAI will be skipped.")

    def clean_text(self, text) -> str:
        """Clean text by decoding if bytes and stripping symbols."""
        if isinstance(text, bytes):
            text = text.decode("utf-8", errors="ignore")
        return (text or "").strip(strip_symbols)

    def standardize_language(self, lang: str) -> str:
        """Standardize language code to ISO 639-1."""
        lang = lang.lower().strip()
        if not lang:
            return "unknown"
        if lang in self.lang_cache:
            return self.lang_cache[lang]

        mapping = {
            "ru": "ru", "rus": "ru", "russian": "ru",
            "en": "en", "eng": "en", "english": "en",
            "de": "de", "ger": "de", "german": "de",
            "fr": "fr", "fre": "fr", "french": "fr",
            # Add more mappings if needed
        }
        if lang in mapping:
            self.lang_cache[lang] = mapping[lang]
            return mapping[lang]

        if len(lang) == 2 and lang.isalpha():
            self.lang_cache[lang] = lang
            return lang

        if self.openai:
            try:
                response = self.openai.chat.completions.create(
                    model="gpt-4.1-nano",  # Corrected to a valid model
                    messages=[{
                        "role": "user",
                        "content": f"What is the ISO 639-1 code for the language '{lang}'? Respond only with the code."
                    }],
                    max_tokens=1000,
                )
                code = response.choices[0].message.content.strip().lower()
                if len(code) == 2 and code.isalpha():
                    self.lang_cache[lang] = code
                    return code
            except Exception as e:
                logger.error(f"Language resolution error for '{lang}': {e}")
        return "unknown"

    def detect_langdetect(self, text: str) -> str:
        """Detect language using langdetect."""
        try:
            return langdetect_detect(text[:500])
        except Exception:
            return "unknown"

    def detect_langid(self, text: str) -> str:
        """Detect language using langid."""
        try:
            lang, _ = langid.classify(text[:500])
            return lang
        except Exception:
            return "unknown"

    def detect_openai(self, text: str) -> str:
        """Detect language using OpenAI as fallback."""
        if not self.openai:
            return "unknown"
        try:
            response = self.openai.chat.completions.create(
                model="gpt-4.1-nano",
                messages=[{
                    "role": "user",
                    "content": f"Detect the ISO 639-1 language code of the following text. Respond only with the 2-letter code.\n\n{text[:1000]}"
                }],
                max_tokens=1000,
            )
            code = response.choices[0].message.content.strip().lower()
            if len(code) == 2 and code.isalpha():
                return code
        except Exception as e:
            logger.error(f"OpenAI language detection failed: {e}")
        return "unknown"

    def determine_language(self, lang_from_tag: str, sample: str) -> str:
        """Determine final language using multi-detector logic with conflict resolution."""
        lang1 = self.detect_langdetect(sample)
        lang2 = self.detect_langid(sample)

        lang_final = None

        # If tag and both detectors match — use it
        if lang_from_tag == lang1 == lang2:
            lang_final = lang_from_tag

        # If tag matches one detector — use tag
        elif lang_from_tag in [lang1, lang2]:
            lang_final = lang_from_tag

        # If all different and not equivalent pairs — use OpenAI
        elif (lang_from_tag, lang1) not in self.equivalent_pairs and (lang_from_tag, lang2) not in self.equivalent_pairs:
            lang_openai = self.detect_openai(sample)
            if lang_openai != "unknown":
                logger.info(f"Language conflict resolved with OpenAI: tag={lang_from_tag}, ld={lang1}, lid={lang2}, openai={lang_openai}")
                lang_final = lang_openai

        # If both detectors match but not tag, and not equivalent — use detectors
        elif lang1 == lang2 and lang1 != lang_from_tag and (lang_from_tag, lang1) not in self.equivalent_pairs:
            logger.info(f"Language conflict: tag={lang_from_tag}, detectors={lang1} (used)")
            lang_final = lang1

        # Fallback to tag if no resolution
        if not lang_final:
            lang_final = lang_from_tag

        return lang_final if lang_final != "unknown" else lang_from_tag  # Avoid "unknown" if possible

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

        try:
            session.add(OpdsCatalogCatalog(cat_name=archive_name, is_scanned=True))
            session.commit()
        except Exception as e:
            session.rollback()
            logger.error(f"Failed to mark archive {archive_name} as scanned: {e}")

    def process_book(self, file_name: str, archive_name: str, book, session) -> None:
        """Process a single book file from the archive, including advanced language detection."""
        try:
            zipped_book = FB2Sax(book, file_name)
        except Exception as e:
            logger.error(f"Failed to parse book {file_name}: {e}")
            return

        # Prepare book metadata
        annotation = self.clean_text(zipped_book.description)[:1000]
        title = self.clean_text(zipped_book.title) or file_name

        # Language detection logic
        raw_lang = self.clean_text(zipped_book.language_code)
        lang_from_tag = self.standardize_language(raw_lang)

        # Combine annotation and body_sample if annotation is too short
        sample = annotation
        if len(sample) < 100:
            remaining_length = 1000 - len(sample)
            sample += self.clean_text(zipped_book.body_sample)[:remaining_length]

        if not sample or len(sample) < 10:
            logger.info(f"{file_name}: text too short for detection, using tag: {lang_from_tag}")
            final_lang = lang_from_tag
        else:
            final_lang = self.determine_language(lang_from_tag, sample)

        try:
            book_object = OpdsCatalogBook(
                filename=file_name,
                path=archive_name,
                format="fb2",
                registerdate=datetime.now(),
                docdate=self.clean_text(zipped_book.docdate),
                lang=final_lang,
                title=title,
                annotation=annotation,
            )
            session.add(book_object)
            session.flush()  # To get book_object.id for relations

            self.process_cover(zipped_book, book_object, archive_name, file_name)
            self.process_authors(zipped_book, book_object, session)
            self.process_series(zipped_book, book_object, session)

            session.commit()
            logger.info(f"Successfully committed book {file_name}")
        except Exception as e:
            session.rollback()
            logger.error(f"Failed to commit book {file_name}: {e}")

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
            logger.info(f"Found {len(unscanned_archives)} unscanned archives")
            for archive_path in unscanned_archives:
                self.scan_archive(archive_path, session)

if __name__ == "__main__":
    processor = BookProcessor()
    processor.process()