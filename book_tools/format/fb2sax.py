import base64
import logging
import traceback
from typing import Any, Dict, Generator, List, Optional

from lxml import etree
from book_tools.format.bookfile import BookFile
from book_tools.format.mimetype import Mimetype
from book_tools.format.util import strip_symbols

# Constants for tag paths
DESCRIPTION_TAG = "description"
BODY_TAG = "body"
AUTHOR_TAG = "author"


class TagHandler:
    """
    Base class for handling specific XML tag paths in a stateful manner.
    Tracks nesting and collects values/attributes for matching paths.
    """

    def __init__(self, tags: List[str]):
        self.tags: List[str] = tags
        self.path_size: int = len(tags)
        self.current_index: int = -1
        self.values: List[str] = []
        self.attributes_list: List[Dict[str, str]] = []
        self.current_attributes: Dict[str, str] = {}
        self.processing_value: bool = False
        self.current_value: str = ""

    def reset(self) -> None:
        """Reset the handler state."""
        self.current_index = -1
        self.values = []
        self.attributes_list = []
        self.current_attributes = {}
        self.processing_value = False
        self.current_value = ""

    def open_tag(self, tag: str, attrs: Optional[Dict[str, str]] = None) -> bool:
        """Handle opening tag; return True if full path matched."""
        if attrs is None:
            attrs = {}
        matched = False
        if self.current_index + 1 < self.path_size and self.tags[self.current_index + 1] == tag:
            self.current_index += 1
        if self.current_index + 1 == self.path_size:
            self.current_attributes = attrs
            self.attributes_list.append(attrs)
            matched = True
        return matched

    def close_tag(self, tag: str) -> None:
        """Handle closing tag; collect value if processing."""
        if self.current_index >= 0 and self.tags[self.current_index] == tag:
            self.current_index -= 1
            if self.processing_value:
                self.values.append(self.current_value.strip())
                self.processing_value = False
                self.current_value = ""

    def set_value(self, data: str) -> None:
        """Accumulate text data if at target depth."""
        if self.current_index + 1 == self.path_size:
            if not self.processing_value:
                self.current_value = data
                self.processing_value = True
            else:
                self.current_value += data

    def get_values(self) -> List[str]:
        """Get collected values."""
        return self.values

    def get_values_generator(self) -> Generator[str, None, None]:
        """Yield collected values lazily."""
        for value in self.values:
            yield value

    def get_text(self, divider: str = "\n") -> str:
        """Join values with divider."""
        return divider.join(self.get_values())

    def get_attribute(self, attr: str) -> Optional[str]:
        """Get attribute from the last matched tag."""
        return self.current_attributes.get(attr)

    def get_attributes(self, attr: str) -> List[Optional[str]]:
        """Get list of attribute values from all matched tags."""
        return [a.get(attr) for a in self.attributes_list if attr in a]


class CoverHandler(TagHandler):
    """
    Specialized handler for cover images, extending TagHandler.
    Collects base64 data for matching cover ID.
    """

    def __init__(self, tags: List[str]):
        super().__init__(tags)
        self.is_cover: bool = False
        self.cover_name: str = ""
        self.cover_data_chunks: List[str] = []
        self.is_found: bool = False

    def reset(self) -> None:
        """Reset cover-specific state."""
        super().reset()
        self.is_cover = False
        self.cover_name = ""
        self.cover_data_chunks = []
        self.is_found = False

    def open_tag(self, tag: str, attrs: Optional[Dict[str, str]] = None) -> bool:
        """Handle opening tag and check for cover ID."""
        matched = super().open_tag(tag, attrs)
        if matched:
            id_value = self.get_attribute("id")
            if id_value and id_value.lower() == self.cover_name.lower():
                self.is_cover = True
        return matched

    def close_tag(self, tag: str) -> None:
        """Handle closing tag and mark if cover was found."""
        if self.is_cover:
            self.is_found = True
            self.is_cover = False
        super().close_tag(tag)

    def set_cover_name(self, cover_name: Optional[str]) -> None:
        """Set the target cover name."""
        self.cover_name = cover_name.strip("#").lower() if cover_name and cover_name.startswith("#") else ""

    def add_data(self, data: str) -> None:
        """Add data chunk if processing cover."""
        if self.is_cover and data != "\n":
            self.cover_data_chunks.append(data)

    @property
    def cover_data(self) -> str:
        """Get joined cover data."""
        return "".join(self.cover_data_chunks)

    @cover_data.setter
    def cover_data(self, value: str) -> None:
        self.cover_data_chunks = [value]


def normalize_tag_name(tag: str) -> str:
    """Normalize XML tag name by removing namespaces and prefixes."""
    return tag.split("}")[-1].lstrip(":").lower() if "}" in tag else tag.lower()


def normalize_attrs(attrs: Dict[str, Any]) -> Dict[str, str]:
    """Normalize attribute keys by removing namespaces."""
    return {normalize_tag_name(k): str(v) for k, v in attrs.items()}


class FB2Parser:
    """
    SAX-like parser target for FB2 files.
    Collects metadata and optional cover data.
    """

    def __init__(self, read_cover: bool = False):
        self.read_cover: bool = read_cover
        self.logger = logging.getLogger(__name__)
        self.tag_handlers: Dict[str, TagHandler] = {
            "author_first": TagHandler(["description", "title-info", "author", "first-name"]),
            "author_last": TagHandler(["description", "title-info", "author", "last-name"]),
            "genre": TagHandler(["description", "title-info", "genre"]),
            "lang": TagHandler(["description", "title-info", "lang"]),
            "book_title": TagHandler(["description", "title-info", "book-title"]),
            "annotation": TagHandler(["description", "title-info", "annotation", "p"]),
            "docdate": TagHandler(["description", "document-info", "date"]),
            "series": TagHandler(["description", "title-info", "sequence"]),
        }
        if read_cover:
            self.tag_handlers["cover_name"] = TagHandler(
                ["description", "title-info", "coverpage", "image"]
            )
            self.tag_handlers["cover_image"] = CoverHandler(["binary"])
        self.stop_tag: str = DESCRIPTION_TAG
        self.processing_description: bool = True
        self.parse_error: int = 0
        self.parse_error_msg: str = ""
        self.in_body: bool = False
        self.body_chunks: List[str] = []

    def reset(self) -> None:
        """Reset parser state."""
        self.processing_description = True
        self.parse_error = 0
        self.parse_error_msg = ""
        self.in_body = False
        self.body_chunks = []
        for handler in self.tag_handlers.values():
            handler.reset()

    def start(self, name: str, attrs: Dict[str, Any]) -> None:
        """Handle start tag event."""
        local_name = normalize_tag_name(name)
        normalized_attrs = normalize_attrs(attrs)
        self.logger.debug(f"Start tag: {local_name}, attrs: {normalized_attrs}")

        if local_name == BODY_TAG:
            self.in_body = True

        if self.processing_description:
            for handler in self.tag_handlers.values():
                if not isinstance(handler, CoverHandler) or not self.read_cover:
                    handler.open_tag(local_name, normalized_attrs)

            if self.read_cover and "cover_name" in self.tag_handlers:
                if self.tag_handlers["cover_name"].open_tag(local_name, normalized_attrs):
                    href = self.tag_handlers["cover_name"].get_attribute("href")
                    self.tag_handlers["cover_image"].set_cover_name(href)  # type: ignore

        if self.read_cover and "cover_image" in self.tag_handlers:
            self.tag_handlers["cover_image"].open_tag(local_name, normalized_attrs)  # type: ignore

    def end(self, name: str) -> None:
        """Handle end tag event."""
        local_name = normalize_tag_name(name)
        self.logger.debug(f"End tag: {local_name}")

        if local_name == BODY_TAG:
            self.in_body = False

        if self.processing_description:
            for handler in self.tag_handlers.values():
                if not isinstance(handler, CoverHandler) or not self.read_cover:
                    handler.close_tag(local_name)

            if self.read_cover and "cover_name" in self.tag_handlers:
                self.tag_handlers["cover_name"].close_tag(local_name)

        if self.read_cover and "cover_image" in self.tag_handlers:
            cover_image: CoverHandler = self.tag_handlers["cover_image"]  # type: ignore
            cover_image.close_tag(local_name)
            if cover_image.is_found:
                raise StopIteration("Cover found, stopping parse")

        if local_name == AUTHOR_TAG:
            first_values = self.tag_handlers["author_first"].get_values()
            last_values = self.tag_handlers["author_last"].get_values()
            if len(last_values) > len(first_values):
                first_values.append(" ")
            elif len(last_values) < len(first_values):
                last_values.append(" ")

        if local_name == self.stop_tag:
            if self.read_cover and self.tag_handlers["cover_image"].cover_name == "":
                raise StopIteration("No cover, stopping parse")
            else:
                self.processing_description = False

    def data(self, data: str) -> None:
        """Handle text data event in the XML parser."""
        self.logger.debug(f"Data in {'body' if self.in_body else 'other'}: {repr(data)}")
        if self.in_body:
            self.body_chunks.append(data)

        if self.processing_description:
            for handler in self.tag_handlers.values():
                if not isinstance(handler, CoverHandler):
                    handler.set_value(data)

        if self.read_cover and "cover_image" in self.tag_handlers:
            self.tag_handlers["cover_image"].add_data(data)  # type: ignore

    def close(self) -> None:
        """Handle parser close."""
        self.logger.debug("Parser closed")
        return None

    def parse(self, file_obj: Any, header_size: int = 0) -> None:
        """Parse the file content."""
        self.reset()
        try:
            parser = etree.XMLParser(target=self, recover=True)
            if header_size == 0:
                source = file_obj.read()
            else:
                source = file_obj.read(header_size)
            parser.feed(source)
            parser.close()
            self.logger.debug(
                f"Parsed data: title={self.tag_handlers['book_title'].get_values()}, "
                f"lang={self.tag_handlers['lang'].get_values()}, "
                f"annotation={self.tag_handlers['annotation'].get_values()}, "
                f"body_length={len(self.get_body_text())}"
            )
            if parser.error_log:
                self.logger.warning(f"Parser warnings: {parser.error_log}")
        except StopIteration:
            self.logger.debug("Parsing stopped early via StopIteration")
        except Exception as err:
            self.parse_error_msg = str(err)
            self.parse_error = 1
            self.logger.error(f"Parsing error: {err}")

    def get_body_text(self) -> str:
        """Get stripped body text."""
        return "".join(self.body_chunks).strip()


class FB2StructureException(Exception):
    """Exception for FB2 structure validation failures."""

    def __init__(self, error: Any):
        super().__init__(f"FB2 verification failed: {error}")
        if isinstance(error, Exception):
            traceback.print_exc()


class FB2Sax(BookFile):
    """
    FB2 file handler extending BookFile.
    Parses and extracts metadata using FB2Parser.
    """

    def __init__(self, file_obj: Any, original_filename: str):
        super().__init__(file_obj, original_filename, Mimetype.FB2)
        self.parser = FB2Parser(read_cover=False)
        self.file.seek(0, 0)
        self.parser.parse(self.file)
        if self.parser.parse_error != 0:
            raise FB2StructureException(f"FB2Sax parse error ({self.parser.parse_error_msg})")
        self._detect_title()
        self._detect_authors()
        self._detect_tags()
        self._detect_series_info()
        self._detect_language()
        self._detect_docdate()
        self.description = self._detect_description()
        self.body_sample = self.parser.get_body_text()

    def extract_cover_memory(self) -> Optional[bytes]:
        """Extract cover image data as bytes."""
        cover_parser = FB2Parser(read_cover=True)
        self.file.seek(0, 0)
        cover_parser.parse(self.file)
        if cover_parser.tag_handlers["cover_image"].cover_data:  # type: ignore
            try:
                data = cover_parser.tag_handlers["cover_image"].cover_data  # type: ignore
                return base64.b64decode(data)
            except Exception as e:
                logging.getLogger(__name__).error(f"Failed to decode cover image: {e}")
        return None

    def _detect_title(self) -> None:
        """Detect and set book title."""
        titles = self.parser.tag_handlers["book_title"].get_values()
        if titles:
            self.__set_title__(titles[0].strip(strip_symbols))

    def _detect_docdate(self) -> None:
        """Detect and set document date."""
        date_attr = self.parser.tag_handlers["docdate"].get_attribute("value") or ""
        if not date_attr and self.parser.tag_handlers["docdate"].get_values():
            date_attr = self.parser.tag_handlers["docdate"].get_values()[0].strip()
        if date_attr:
            self.__set_docdate__(date_attr)

    def _detect_authors(self) -> None:
        """Detect and add authors."""
        first_names = self.parser.tag_handlers["author_first"].get_values()
        last_names = self.parser.tag_handlers["author_last"].get_values()
        for idx, last_name in enumerate(last_names):
            last_name = last_name.strip(strip_symbols)
            first_name = first_names[idx].strip(strip_symbols) if idx < len(first_names) else ""
            full_name = " ".join([first_name, last_name]).strip()
            self.__add_author__(full_name, last_name)

    def _detect_language(self) -> None:
        """Detect and set language code."""
        langs = self.parser.tag_handlers["lang"].get_values()
        if langs:
            self.language_code = langs[0].strip(strip_symbols)

    def _detect_tags(self) -> None:
        """Detect and add genres as tags."""
        for genre in self.parser.tag_handlers["genre"].get_values():
            self.__add_tag__(genre.lower().strip(strip_symbols))

    def _detect_series_info(self) -> None:
        """Detect series title and index."""
        series_attrs = self.parser.tag_handlers["series"].attributes_list
        if series_attrs:
            attr = series_attrs[0]
            ser_name = attr.get("name")
            if ser_name:
                title = ser_name.strip(strip_symbols)
                index = attr.get("number", "0").strip(strip_symbols)
                self.series_info = {"title": title, "index": index}

    def _detect_description(self) -> Optional[str]:
        """Detect annotation as description."""
        annotations = self.parser.tag_handlers["annotation"].get_values()
        if annotations:
            return "\n".join(annotations)
        return None