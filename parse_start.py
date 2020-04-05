#import magic
import os
import zipfile
from xml import sax
from io import BytesIO
import glob

from book_tools.format.mimetype import Mimetype

from book_tools.format.util import list_zip_file_infos
from book_tools.format.epub import EPub
from book_tools.format.fb2 import FB2, FB2Zip
from book_tools.format.fb2sax import FB2sax
from book_tools.format.other import Dummy
from book_tools.format.mobi import Mobipocket

class mime_detector:
    @staticmethod
    def fmt(fmt):
        if fmt.lower() == 'xml':
            return Mimetype.XML
        elif fmt.lower() == 'fb2':
            return Mimetype.FB2
        elif fmt.lower() =='epub':
            return Mimetype.EPUB
        elif fmt.lower() =='mobi':
            return Mimetype.MOBI
        elif fmt.lower() == 'zip':
            return Mimetype.ZIP
        elif fmt.lower() =='pdf':
            return Mimetype.PDF
        elif fmt.lower() =='doc' or fmt.lower()=='docx':
            return Mimetype.MSWORD
        elif fmt.lower() =='djvu':
            return Mimetype.DJVU
        elif fmt.lower() =='txt':
            return Mimetype.TEXT
        elif fmt.lower() =='rtf':
            return Mimetype.RTF
        else:
            return Mimetype.OCTET_STREAM

    @staticmethod
    def file(filename):
        (n, e) = os.path.splitext(filename)
        return mime_detector.fmt(e[1:])

def detect_mime(file, original_filename):
    FB2_ROOT = 'FictionBook'
    mime = mime_detector.file(original_filename)

    try:
        if mime == Mimetype.XML:
            if FB2_ROOT == __xml_root_tag(file):
                return Mimetype.FB2
        elif mime == Mimetype.ZIP:
            with zipfile.ZipFile(file) as zip_file:
                if not zip_file.testzip():
                    infolist = list_zip_file_infos(zip_file)
                    if len(infolist) == 1:
                        if FB2_ROOT == __xml_root_tag(zip_file.open(infolist[0])):
                            return Mimetype.FB2_ZIP
                    try:
                        with zip_file.open('mimetype') as mimetype_file:
                            if mimetype_file.read(30).decode().rstrip('\n\r') == Mimetype.EPUB:
                                return Mimetype.EPUB
                    except Exception as e:
                        pass
        elif mime == Mimetype.OCTET_STREAM:
            mobiflag =  file.read(68)
            mobiflag = mobiflag[60:]
            if mobiflag.decode() == 'BOOKMOBI':
                return Mimetype.MOBI
    except:
        pass

    return mime

def create_bookfile(file, original_filename):
    if isinstance(file, str):
        file = open(file, 'rb')
    file = BytesIO(file.read())
    mimetype = detect_mime(file,original_filename)
    if mimetype == Mimetype.EPUB:
        return EPub(file, original_filename)
    elif mimetype == Mimetype.FB2:
        return FB2sax(file, original_filename) if config.SOPDS_FB2SAX else FB2(file, original_filename)
    elif mimetype == Mimetype.FB2_ZIP:
        return FB2Zip(file, original_filename)
    elif mimetype == Mimetype.MOBI:
        return Mobipocket(file, original_filename)
    elif mimetype in [Mimetype.TEXT, Mimetype.PDF, Mimetype.MSWORD, Mimetype.RTF, Mimetype.DJVU]:
       return Dummy(file, original_filename, mimetype)
    else:
        raise Exception('File type \'%s\' is not supported, sorry' % mimetype)

def __xml_root_tag(file):
    class XMLRootFound(Exception):
        def __init__(self, name):
            self.name = name

    class RootTagFinder(sax.handler.ContentHandler):
        def startElement(self, name, attributes):
            raise XMLRootFound(name)

    try:
        sax.parse(file, RootTagFinder())
    except XMLRootFound as e:
        return e.name
    return None

if __name__ == "__main__":
    # zip = zipfile.ZipFile('books/fb2-153556-158325.zip')
    # for n in zip.namelist():
    #
    #     f = zip.open(n)
    #     zipped_book = FB2sax(f, n)
    #     print(zipped_book.authors)
    #     print(zipped_book.description)
    #     print(zipped_book.series_info)
    #     print(zipped_book.docdate)
    #     print(zipped_book.language_code)
    #     print(zipped_book.title)
    #     print("\n\n\n")

    # Получение папки с архивами из конфига
    archives_list = glob.glob("books/*.zip")

    # Запрос в бд списка архивов, которые уже были отсканированы
    # Здесь будет запрос
    print(archives_list)

    # Сопоставили, получили только те, которые надо отсканировать
    unscanned_archives = archives_list

    # Проход по списку из полученных архивов
    for archive_name in unscanned_archives:
        # Записываем архив в базу как несканированный

        # Открываем архив
        scan_it = zipfile.ZipFile(archive_name)

        # Проход по файлам в архиве
        for f in scan_it.namelist():

            # Открываем файл из архива
            book = scan_it.open(f)

            # Инициализируем класс для сканирования
            zipped_book = FB2sax(book, f)



            # Создаем объект книги для таблицы opds_catalog_book
            book_object = {
                "filename": f,
                "path": archive_name,
                "format": "fb2",
                "docdate": zipped_book.docdate,
                "lang": zipped_book.language_code,
                "title": zipped_book.title,
                "annotation": zipped_book.description,
            }

            # Записываем объект в базу и получаем его ID

            # Получаем список авторов из книги
            authors = zipped_book.authors

            for author in authors:
                # Ищем автора в opds_catalog_author

                # Если не находим автора, записываем его в таблицу и получаем его ID

                # Записываем связку в opds_catalog_bauthor
                print(author)

            # Получаем список серий из книги
            series = zipped_book.series_info

            # Записываем книгу в серию opds_catalog_bseries
            print(series)

            print(book_object, "\n\n\n\n")

        # Помечаем архив в базе как сканированный