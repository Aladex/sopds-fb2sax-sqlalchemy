# import magic
import errno
import glob
import os
import zipfile
from datetime import datetime

import yaml
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from book_tools.format.fb2sax import FB2sax
from book_tools.format.util import strip_symbols
from models.models import OpdsCatalogBook, OpdsCatalogCatalog, OpdsCatalogAuthor, OpdsCatalogSery, OpdsCatalogBauthor, \
    OpdsCatalogBsery


def get_or_create(session, model, **kwargs):
    instance = session.query(model).filter_by(**kwargs).first()
    if instance:
        return instance
    else:
        instance = model(**kwargs)
        session.add(instance)
        return instance


if __name__ == "__main__":
    scan_conf = yaml.safe_load(open("config.yaml", 'r'))
    books_path = scan_conf["path_to_archives"]

    DB_URL = "postgresql+psycopg2://sopds:sopds@127.0.0.1/sopds"
    engine = create_engine(DB_URL)
    Session = sessionmaker(bind=engine)
    session = Session(autocommit=True)

    # Получение папки с архивами из конфига
    archives_list = [a.replace(books_path, "") for a in glob.glob(books_path + "/*.zip")]

    # Запрос в бд списка архивов, которые уже были отсканированы
    # Здесь будет запрос
    db_archives = [a.cat_name for a in session.query(OpdsCatalogCatalog).all()]

    # Сопоставили, получили только те, которые надо отсканировать
    # unscanned_archives = archives_list
    excluded_archives = [a for a in archives_list if a not in db_archives]
    for archive_name in excluded_archives:
        print(archive_name)
        session.add(OpdsCatalogCatalog(
            cat_name=archive_name,
            is_scanned=True
        ))
        # Открываем архив
        scan_it = zipfile.ZipFile(books_path + archive_name)
        for f in scan_it.namelist():

            # Открываем файл из архива
            book = scan_it.open(f)

            # Инициализируем класс для сканирования
            zipped_book = FB2sax(book, f)
            annotation = zipped_book.description if zipped_book.description else ''
            annotation = annotation.strip(strip_symbols) if isinstance(annotation, str) else annotation.decode(
                'utf8').strip(strip_symbols)

            book_object = OpdsCatalogBook(
                filename=f,
                path=archive_name,
                format="fb2",
                registerdate=datetime.now(),
                docdate=zipped_book.docdate if zipped_book.docdate else '',
                lang=zipped_book.language_code.strip(strip_symbols) if zipped_book.language_code else '',
                title=zipped_book.title.strip(strip_symbols) if zipped_book.title else f,
                annotation=annotation
            )

            # Попытаться извлечь обложку
            cover_extr = zipped_book.extract_cover_memory()
            if cover_extr is not None:
                filename = scan_conf.get("path_to_covers") + archive_name.replace(".", "-") + "/" + f.replace(".",
                                                                                                              "-") + ".jpg"
                print(filename)

                if not os.path.exists(os.path.dirname(filename)):
                    try:
                        os.makedirs(os.path.dirname(filename))
                    except OSError as exc:  # Guard against race condition
                        if exc.errno != errno.EEXIST:
                            raise
                cover = open(filename, "wb")
                cover.write(cover_extr)
                cover.close()
                book_object.cover = True

            # Добавляем книгу в БД
            session.add(book_object)

            authors = zipped_book.authors
            books_bauthors = list()
            # Если автора нет, добавляем пустой словарь
            if len(authors) == 0:
                authors.append({})
            for author in authors:
                # author_name = author.get("name", None)
                author_name = author.get('name', 'Author Unknown').strip(strip_symbols)
                # Если в имени автора нет запятой, то фамилию переносим из конца в начало
                if author_name and author_name.find(',') < 0:
                    author_names = author_name.split()
                    author_name = ' '.join([author_names[-1], ' '.join(author_names[:-1])])
                author = get_or_create(session, OpdsCatalogAuthor, full_name=author_name)
                books_bauthors.append(author.id)

            for ab in books_bauthors:
                session.add(OpdsCatalogBauthor(
                    author_id=ab,
                    book_id=book_object.id
                ))

            # ПОИСК СЕРИЙ
            books_bseries = list()
            if zipped_book.series_info:
                ser = get_or_create(session, OpdsCatalogSery, ser=zipped_book.series_info['title'])
                ser_no = zipped_book.series_info['index'] or '0'
                ser_no = int(ser_no) if ser_no.isdigit() else 0
                books_bseries.append((ser_no, ser.id))
                # Записать связи с серией!
            for bs in books_bseries:
                session.add(OpdsCatalogBsery(
                    ser_no=bs[0],
                    ser_id=bs[1],
                    book_id=book_object.id
                ))
