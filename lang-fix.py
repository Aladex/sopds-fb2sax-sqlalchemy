import glob
import logging
import os
import zipfile
from typing import Dict

import yaml
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from book_tools.format.fb2sax import FB2sax
from book_tools.format.util import strip_symbols
from models.models import OpdsCatalogBook

from openai import OpenAI
from langdetect import detect

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def load_config(path: str) -> Dict:
    with open(path, "r") as f:
        return yaml.safe_load(f)


class LanguageUpdater:
    def __init__(self, config_path="config.yaml"):
        self.config = load_config(config_path)
        self.engine = create_engine(self.config["db_url"])
        self.Session = sessionmaker(bind=self.engine)
        self.openai = OpenAI(api_key=self.config["openai_api_key"])
        self.lang_cache = {}

    def clean(self, text) -> str:
        if isinstance(text, bytes):
            text = text.decode("utf-8", errors="ignore")
        return (text or "").strip(strip_symbols).lower()

    def standardize_language(self, lang: str) -> str:
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
        }
        if lang in mapping:
            self.lang_cache[lang] = mapping[lang]
            return mapping[lang]

        if len(lang) == 2 and lang.isalpha():
            self.lang_cache[lang] = lang
            return lang

        try:
            response = self.openai.chat.completions.create(
                model="gpt-4.1-nano",
                messages=[{
                    "role": "user",
                    "content": f"What is the ISO 639-1 code for the language '{lang}'? Respond only with the code."
                }],
                max_tokens=5,
            )
            code = response.choices[0].message.content.strip().lower()
            if len(code) == 2 and code.isalpha():
                self.lang_cache[lang] = code
                return code
        except Exception as e:
            logger.error(f"Language resolution error for '{lang}': {e}")

        return "unknown"

    def detect_language_from_text(self, text: str) -> str:
        try:
            return detect(text[:500])
        except Exception:
            return "unknown"

    def update_languages(self) -> None:
        base_path = self.config["path_to_archives"]
        session = self.Session()
        try:
            books = session.query(OpdsCatalogBook).all()
            logger.info(f"Loaded {len(books)} books")

            updated = 0
            for book in books:
                archive_path = os.path.join(base_path, book.path)
                if not os.path.exists(archive_path):
                    logger.warning(f"Missing archive: {archive_path}")
                    continue
                try:
                    with zipfile.ZipFile(archive_path) as z:
                        with z.open(book.filename) as f:
                            parsed = FB2sax(f, book.filename)
                            raw_lang = self.clean(parsed.language_code)
                            lang_from_tag = self.standardize_language(raw_lang)

                            annotation = self.clean(parsed.description)[:1000]
                            sample = annotation or parsed.title or ""

                            lang_from_text = self.detect_language_from_text(sample)

                            if lang_from_tag != lang_from_text and lang_from_text != "unknown":
                                logger.info(f"{book.filename} ({book.id}): tag={lang_from_tag}, detected={lang_from_text}")
                                book.lang = lang_from_text
                                session.commit()
                                updated += 1
                            elif lang_from_tag != book.lang:
                                logger.info(f"{book.filename} ({book.id}): correcting lang from {book.lang} to {lang_from_tag}")
                                book.lang = lang_from_tag
                                session.commit()
                                updated += 1
                except Exception as e:
                    logger.error(f"Error reading {book.filename} in {book.path}: {e}")
                    session.rollback()
                    continue

            logger.info(f"Updated language for {updated} books")
        except Exception as e:
            logger.error(f"Critical failure: {e}")
            session.rollback()
        finally:
            session.close()


if __name__ == "__main__":
    LanguageUpdater().update_languages()
