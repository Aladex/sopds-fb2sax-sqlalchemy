# coding: utf-8
from sqlalchemy import Boolean, Column, DateTime, ForeignKey, Integer, String, Table, text
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()
metadata = Base.metadata


class OpdsCatalogAuthor(Base):
    __tablename__ = 'opds_catalog_author'

    id = Column(Integer, primary_key=True)
    full_name = Column(String(128), nullable=False, index=True)


class OpdsCatalogBook(Base):
    __tablename__ = 'opds_catalog_book'

    id = Column(Integer, primary_key=True)
    filename = Column(String(512), nullable=False, index=True)
    path = Column(String(512), nullable=False, index=True)
    format = Column(String(8), nullable=False)
    registerdate = Column(DateTime(True), nullable=False)
    docdate = Column(String(32), nullable=False, index=True)
    lang = Column(String(16), nullable=False)
    title = Column(String(512), nullable=False, index=True)
    annotation = Column(String(10000), nullable=False)
    cover = Column(Boolean, default=False)


class OpdsCatalogCatalog(Base):
    __tablename__ = 'opds_catalog_catalog'

    id = Column(Integer, primary_key=True)
    cat_name = Column(String(190), nullable=False, index=True)
    is_scanned = Column(Boolean, default=False)


class OpdsCatalogSery(Base):
    __tablename__ = 'opds_catalog_series'

    id = Column(Integer, primary_key=True)
    ser = Column(String(150), nullable=False, index=True)
    lang_code = Column(Integer, nullable=False, index=True)


class OpdsCatalogBauthor(Base):
    __tablename__ = 'opds_catalog_bauthor'

    id = Column(Integer, primary_key=True)
    author_id = Column(ForeignKey('opds_catalog_author.id', deferrable=True, initially='DEFERRED'), nullable=False, index=True)
    book_id = Column(ForeignKey('opds_catalog_book.id', deferrable=True, initially='DEFERRED'), nullable=False, index=True)

    author = relationship('OpdsCatalogAuthor')
    book = relationship('OpdsCatalogBook')


class OpdsCatalogBsery(Base):
    __tablename__ = 'opds_catalog_bseries'

    id = Column(Integer, primary_key=True)
    ser_no = Column(Integer, nullable=False)
    book_id = Column(ForeignKey('opds_catalog_book.id', deferrable=True, initially='DEFERRED'), nullable=False, index=True)
    ser_id = Column(ForeignKey('opds_catalog_series.id', deferrable=True, initially='DEFERRED'), nullable=False, index=True)

    book = relationship('OpdsCatalogBook')
    ser = relationship('OpdsCatalogSery')
