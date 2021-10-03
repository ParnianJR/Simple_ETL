CREATE DATABASE ""
    WITH 
    OWNER = postgres
    ENCODING = 'UTF8'
    CONNECTION LIMIT = -1;
 
 --create tables:
  
 create table Book_details(
	book_id		int			NOT NULL,
	ISBN			varchar		NOT NULL,
	publish_date		DATE			NOT NULL,
	constraint BOOKD_PK primary key(book_id),
	constraint ISBN_CHECK check(ISBN like '^(?=(?:\D*\d){10}(?:(?:\D*\d){3})?$)[\d-]+$)')
);

create table ISBN_details(
	ISBN			varchar	NOT null,
	title			varchar	NOT null,
	description		varchar		,
	publisher		varchar	NOT null,
	edition_num		integer	NOT NULL,
	constraint ISBN_PK primary key(ISBN)
);

CREATE TABLE Book_ISBN(
	book_id		int			NOT NULL,
	ISBN			varchar		check(ISBN like '^(?=(?:\D*\d){10}(?:(?:\D*\d){3})?$)[\d-]+$)'),
	constraint BOOK_ISBN_PK primary key(book_id,ISBN),
	constraint BOOK_ISBN_FK1 foreign key(book_id) references Book_details(book_id),
	constraint BOOK_ISBN_FK2 foreign key(ISBN) references ISBN_details(ISBN)
);


create table Writer(
	writer_id	int		not NULL,
	writer_name	varchar	not NULL,
	constraint WRITER_PK primary key(writer_id)
);



create table Write_(
        write_id		int 	not NULL,
	book_id		int	not NULL,
	writer_id		int	not NULL,
	constraint WRITE_PK primary key(write_id),
	constraint WRITE_FK1 foreign key(book_id) references Book_details(book_id),
	constraint WRITE_FK2 foreign key(writer_id) references Writer(writer_id)
);

create table Language_(
	L_id			int		not NULL,
	L_name			varchar	not NULL,
	constraint LANGUAGE_PK primary key(L_id)
);

create table Language_Book(
	language_book_id	int		not NULL,
	book_id		int		not NULL,
	L_id			int		not NULL,
	constraint LANGUAGE_BOOK_PK primary key(language_book_id),
	constraint LANGUAGE_BOOK_FK1 foreign key(book_id) references Book_details(book_id),
	constraint LANGUAGE_BOOK_FK2 foreign key(L_id) references Language_ (L_id)
);

create table Genres(
	g_id			int		not NULL,
	g_name			varchar	not NULL,
	constraint GENRES_PK primary key(g_id)
);

create table Genre_Book(
	genre_book_id		int		not NULL,
	book_id		int		not NULL,
	g_id			int		not NULL,
	constraint GB_PK primary key(genre_book_id),
	constraint GB_FK1 foreign key (book_id) references Book_details (book_id),
	constraint GB_FK2 foreign key (g_id) references Genres (g_id)
);

create table Translator(
	t_id			int		not NULL,
	t_name			varchar	not NULL,
	constraint TRANSLATOR_PK primary key (t_id)
);

create table Is_translated(
	is_translated_id	int	not NULL,
	book_id		int	not NULL,
	t_id			int	not NULL,
	constraint IS_TRANSLATED_PK primary key(is_translated_id),
	constraint IS_TRANSLATED_FK1 foreign key (book_id) references Book_details (book_id),
	constraint IS_TRANSLATED_FK2 foreign key (t_id) references Translator (t_id)
);

create table Library_member(
	membership_num				int			not NULL,
	Fname					varchar		not NULL,
	Lname					varchar		not NULL,
	birth_date				DATE			not NULL,
	start_membership_date			DATE			not NULL,
	phone_num				int			not NULL,
	address				varchar		not NULL,
	constraint LMEMBER_PK primary key(membership_num)
);

create table Borrow(
	b_id			int			not NULL,
	borrow_num		int			not Null,
	membership_num		int			not NULL,
	book_id		int			not NULL,
	start_borrowing_date	DATE			not NULL,
	scheduled_return_date	DATE			not NULL,
	returned_date		DATE			not NULL,
	constraint BORROW_PK primary key(b_id),
	constraint BORROW_FK1 foreign key(membership_num) references Library_member (membership_num),
	constraint BORROW_FK2 foreign key(book_id) references Book_details (book_id)
);

