CREATE DATABASE "Data_warehouse"
    WITH 
    OWNER = postgres
    ENCODING = 'UTF8'
    CONNECTION LIMIT = -1;
 
 --create tables:
  
 create table Book_details(
	book_id		int			NOT NULL,
	ISBN			varchar		NOT NULL,
	publish_date		DATE			NOT NULL,
	insertion_date		DATE			NOT NULL,
	constraint BOOKD_PK primary key(book_id),
	constraint ISBN_CHECK check(ISBN like '^(?=(?:\D*\d){10}(?:(?:\D*\d){3})?$)[\d-]+$)')
);

create table ISBN_details(
	ISBN			varchar	NOT null,
	title			varchar	NOT null,
	description		varchar			,
	publisher		varchar	NOT null,
	version_num		integer	NOT NULL,
	insertion_date		DATE		NOT NULL,
	constraint ISBN_PK primary key(ISBN)
);

CREATE TABLE Book_ISBN(
	book_id		int			NOT NULL,
	ISBN			varchar		check(ISBN like '^(?=(?:\D*\d){10}(?:(?:\D*\d){3})?$)[\d-]+$)'),
	insertion_date		DATE			NOT NULL,
	constraint BOOK_ISBN_PK primary key(book_id,ISBN),
	constraint BOOK_ISBN_FK1 foreign key(book_id) references Book_details(book_id),
	constraint BOOK_ISBN_FK2 foreign key(ISBN) references ISBN_details(ISBN)
);


create table Writer(
	writer_id	int		not NULL,
	writer_name	varchar	not NULL,
	insertion_date	DATE		not NULL,
	constraint WRITER_PK primary key(writer_id)
);



create table Write_(
        write_id		int 	not NULL,
	book_id		int	not NULL,
	writer_id		int	not NULL,
	insertion_date		DATE		NOT NULL,
	constraint WRITE_PK primary key(write_id),
	constraint WRITE_FK1 foreign key(book_id) references Book_details(book_id),
	constraint WRITE_FK2 foreign key(writer_id) references Writer(writer_id)
);

create table Language_(
	L_id			int		not NULL,
	L_name			varchar	not NULL,
	insertion_date		DATE		NOT NULL,
	constraint LANGUAGE_PK primary key(L_id)
);

create table Language_Book(
	language_book_id	int		not NULL,
	book_id		int		not NULL,
	L_id			int		not NULL,
	insertion_date		DATE		NOT NULL,
	constraint LANGUAGE_BOOK_PK primary key(language_book_id),
	constraint LANGUAGE_BOOK_FK1 foreign key(book_id) references Book_details(book_id),
	constraint LANGUAGE_BOOK_FK2 foreign key(L_id) references Language_ (L_id)
);

create table Genres(
	g_id			int		not NULL,
	g_name			varchar	not NULL,
	insertion_date		DATE		NOT NULL,
	constraint GENRES_PK primary key(g_id)
);

create table Genre_Book(
	genre_book_id		int		not NULL,
	book_id		int		not NULL,
	g_id			int		not NULL,
	insertion_date		DATE		NOT NULL,
	constraint GB_PK primary key(genre_book_id),
	constraint GB_FK1 foreign key (book_id) references Book_details (book_id),
	constraint GB_FK2 foreign key (g_id) references Genres (g_id)
);

create table Translator(
	t_id			int		not NULL,
	t_name			varchar	not NULL,
	insertion_date		DATE		NOT NULL,
	constraint TRANSLATOR_PK primary key (t_id)
);

create table Is_translated(
	is_translated_id	int	not NULL,
	book_id		int	not NULL,
	t_id			int	not NULL,
	insertion_date		DATE		NOT NULL,
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
	insertion_date				DATE			NOT NULL,
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
	insertion_date		DATE			not NULL,
	constraint BORROW_PK primary key(b_id),
	constraint BORROW_FK1 foreign key(membership_num) references Library_member (membership_num),
	constraint BORROW_FK2 foreign key(book_id) references Book_details (book_id)
);





--create history tables for each table above:

CREATE TABLE Book_details_history (LIKE Book_details);
ALTER TABLE Book_details_history
add column modify_date DATE NOT NULL,
add column reason varchar NOT NULL;

CREATE TABLE ISBN_details_history (LIKE ISBN_details);
ALTER TABLE ISBN_details_history
add column modify_date DATE NOT NULL,
add column reason varchar NOT NULL;


CREATE TABLE Book_ISBN_history (LIKE Book_ISBN);
ALTER TABLE Book_ISBN_history
add column modify_date DATE NOT NULL,
add column reason varchar NOT NULL;

CREATE TABLE Writer_history (LIKE Writer);
ALTER TABLE Writer_history
add column modify_date DATE NOT NULL,
add column reason varchar NOT NULL;

CREATE TABLE Write_history (LIKE Write_);
ALTER TABLE Write_history
add column modify_date DATE NOT NULL,
add column reason varchar NOT NULL;

CREATE TABLE Language_history (LIKE Language_);
ALTER TABLE Language_history
add column modify_date DATE NOT NULL,
add column reason varchar NOT NULL;

CREATE TABLE  Language_Book_history (LIKE  Language_Book);
ALTER TABLE  Language_Book_history
add column modify_date DATE NOT NULL,
add column reason varchar NOT NULL;

CREATE TABLE   Genres_history (LIKE  Genres);
ALTER TABLE   Genres_history
add column modify_date DATE NOT NULL,
add column reason varchar NOT NULL;

CREATE TABLE   Genre_Book_history (LIKE  Genre_Book);
ALTER TABLE   Genre_Book_history
add column modify_date DATE NOT NULL,
add column reason varchar NOT NULL;

CREATE TABLE  Translator_history (LIKE  Translator);
ALTER TABLE   Translator_history
add column modify_date DATE NOT NULL,
add column reason varchar NOT NULL;

CREATE TABLE  Is_translated_history (LIKE  Is_translated);
ALTER TABLE   Is_translated_history
add column modify_date DATE NOT NULL,
add column reason varchar NOT NULL;

CREATE TABLE  Library_member_history (LIKE  Library_member);
ALTER TABLE   Library_member_history
add column modify_date DATE NOT NULL,
add column reason varchar NOT NULL;

CREATE TABLE  Borrow_history (LIKE  Borrow);
ALTER TABLE   Borrow_history
add column modify_date DATE NOT NULL,
add column reason varchar NOT NULL;





--create trigger for each table:

CREATE FUNCTION UP_Book_details()
RETURNS TRIGGER
language PLPGSQL
AS
$$
BEGIN
	INSERT INTO Book_details_history values(OLD.* , CURRENT_DATE , 'update');
	RETURN NEW;
END;
$$

CREATE TRIGGER UP_Book_details
BEFORE UPDATE ON Book_details
FOR EACH ROW 
EXECUTE PROCEDURE UP_Book_details();


CREATE FUNCTION DEL_Book_details()
RETURNS TRIGGER
language PLPGSQL
AS
$$
BEGIN
	INSERT INTO Book_details_history values(OLD.* , CURRENT_DATE , 'delete');
	RETURN NEW;
END;
$$



CREATE TRIGGER DEL_Book_details
BEFORE DELETE ON Book_details
FOR EACH ROW 
EXECUTE PROCEDURE DEL_Book_details();



CREATE FUNCTION UP_ISBN_details()
RETURNS TRIGGER
language PLPGSQL
AS
$$
BEGIN
	INSERT INTO ISBN_details_history values(OLD.* , CURRENT_DATE , 'update');
	RETURN NEW;
END;
$$



CREATE TRIGGER UP_ISBN_details
BEFORE UPDATE ON Book_details
FOR EACH ROW 
EXECUTE PROCEDURE UP_ISBN_details();



CREATE FUNCTION DEL_ISBN_details()
RETURNS TRIGGER
language PLPGSQL
AS
$$
BEGIN
	INSERT INTO ISBN_details_history values(OLD.* , CURRENT_DATE , 'delete');
	RETURN NEW;
END;
$$

CREATE TRIGGER DEL_ISBN_details()
BEFORE DELETE ON Book_details
FOR EACH ROW 
EXECUTE PROCEDURE DEL_ISBN_details();


CREATE FUNCTION UP_Book_ISBN()
RETURNS TRIGGER
language PLPGSQL
AS
$$
BEGIN
	INSERT INTO Book_ISBN_history values(OLD.* , CURRENT_DATE , 'update');
	RETURN NEW;
END;
$$

CREATE TRIGGER UP_Book_ISBN()
BEFORE UPDATE ON Book_ISBN
FOR EACH ROW 
EXECUTE PROCEDURE UP_Book_ISBN();


CREATE FUNCTION DEL_Book_ISBN()
RETURNS TRIGGER
language PLPGSQL
AS
$$
BEGIN
	INSERT INTO Book_ISBN_history values(OLD.* , CURRENT_DATE , 'delete');
	RETURN NEW;
END;
$$

CREATE TRIGGER DEL_Book_ISBN()
BEFORE DELETE ON Book_ISBN
FOR EACH ROW 
EXECUTE PROCEDURE DEL_Book_ISBN();


CREATE FUNCTION UP_Writer()
RETURNS TRIGGER
language PLPGSQL
AS
$$
BEGIN
	INSERT INTO Writer_history values(OLD.* , CURRENT_DATE , 'update');
	RETURN NEW;
END;
$$

CREATE TRIGGER UP_Writer
BEFORE UPDATE ON Writer
FOR EACH ROW 
EXECUTE PROCEDURE UP_Writer();


CREATE FUNCTION DEL_Writer()
RETURNS TRIGGER
language PLPGSQL
AS
$$
BEGIN
	INSERT INTO Writer_history values(OLD.* , CURRENT_DATE , 'delete');
	RETURN NEW;
END;
$$

CREATE TRIGGER DEL_Writer
BEFORE DELETE ON Writer
FOR EACH ROW 
EXECUTE PROCEDURE DEL_Writer();


CREATE FUNCTION UP_Write()
RETURNS TRIGGER
language PLPGSQL
AS
$$
BEGIN
	INSERT INTO Write_history values(OLD.* , CURRENT_DATE , 'update');
	RETURN NEW;
END;
$$

CREATE TRIGGER UP_Write
BEFORE UPDATE ON Write_
FOR EACH ROW 
EXECUTE PROCEDURE UP_Write();


CREATE FUNCTION DEL_Write()
RETURNS TRIGGER
language PLPGSQL
AS
$$
BEGIN
	INSERT INTO Write_history values(OLD.* , CURRENT_DATE , 'delete');
	RETURN NEW;
END;
$$

CREATE TRIGGER DEL_Write
BEFORE  DELETE ON Write_
FOR EACH ROW 
EXECUTE PROCEDURE DEL_Write();


CREATE FUNCTION UP_Language()
RETURNS TRIGGER
language PLPGSQL
AS
$$
BEGIN
	INSERT INTO Language_history values(OLD.* , CURRENT_DATE , 'update');
	RETURN NEW;
END;
$$


CREATE TRIGGER UP_Language
BEFORE UPDATE  ON Language_
FOR EACH ROW 
EXECUTE PROCEDURE UP_Language();


CREATE FUNCTION DEL_Language()
RETURNS TRIGGER
language PLPGSQL
AS
$$
BEGIN
	INSERT INTO Language_history values(OLD.* , CURRENT_DATE , 'delete');
	RETURN NEW;
END;
$$

CREATE TRIGGER DEL_Language
BEFORE DELETE ON Language_
FOR EACH ROW 
EXECUTE PROCEDURE DEL_Language();


CREATE FUNCTION UP_Language_Book()
RETURNS TRIGGER
language PLPGSQL
AS
$$
BEGIN
	INSERT INTO Language_Book_history values(OLD.* , CURRENT_DATE , 'update');
	RETURN NEW;
END;
$$

CREATE TRIGGER UP_Language_Book
BEFORE UPDATE  ON Language_Book
FOR EACH ROW 
EXECUTE PROCEDURE UP_Language_Book();


CREATE FUNCTION DEL_Language_Book()
RETURNS TRIGGER
language PLPGSQL
AS
$$
BEGIN
	INSERT INTO Language_Book_history values(OLD.* , CURRENT_DATE , 'delete');
	RETURN NEW;
END;
$$

CREATE TRIGGER DEL_Language_Book
BEFORE UPDATE  ON Language_Book
FOR EACH ROW 
EXECUTE PROCEDURE DEL_Language_Book();


CREATE FUNCTION UP_Genres()
RETURNS TRIGGER
language PLPGSQL
AS
$$
BEGIN
	INSERT INTO Genres_history values(OLD.* , CURRENT_DATE , 'update');
	RETURN NEW;
END;
$$


CREATE TRIGGER UP_Genres
BEFORE UPDATE  ON Genres
FOR EACH ROW 
EXECUTE PROCEDURE UP_Genres();



CREATE FUNCTION DEL_Genres()
RETURNS TRIGGER
language PLPGSQL
AS
$$
BEGIN
	INSERT INTO Genres_history values(OLD.* , CURRENT_DATE , 'delete');
	RETURN NEW;
END;
$$


CREATE TRIGGER DEL_Genres
BEFORE UPDATE  ON Genres
FOR EACH ROW 
EXECUTE PROCEDURE DEL_Genres();



CREATE FUNCTION UP_Genre_Book()
RETURNS TRIGGER
language PLPGSQL
AS
$$
BEGIN
	INSERT INTO Genre_Book_history values(OLD.* , CURRENT_DATE , 'update');
	RETURN NEW;
END;
$$


CREATE TRIGGER UP_Genre_Book
BEFORE UPDATE  ON Genre_Book
FOR EACH ROW 
EXECUTE PROCEDURE UP_Genre_Book();



CREATE FUNCTION DEL_Genre_Book()
RETURNS TRIGGER
language PLPGSQL
AS
$$
BEGIN
	INSERT INTO Genre_Book_history values(OLD.* , CURRENT_DATE , 'delete');
	RETURN NEW;
END;
$$


CREATE TRIGGER DEL_Genre_Book
BEFORE UPDATE  ON Genre_Book
FOR EACH ROW 
EXECUTE PROCEDURE DEL_Genre_Book();



CREATE FUNCTION UP_Translator()
RETURNS TRIGGER
language PLPGSQL
AS
$$
BEGIN
	INSERT INTO Translator_history values(OLD.* , CURRENT_DATE , 'update');
	RETURN NEW;
END;
$$


CREATE TRIGGER UP_Translator
BEFORE UPDATE  ON Translator
FOR EACH ROW 
EXECUTE PROCEDURE UP_Translator();



CREATE FUNCTION DEL_Translator()
RETURNS TRIGGER
language PLPGSQL
AS
$$
BEGIN
	INSERT INTO Translator_history values(OLD.* , CURRENT_DATE , 'delete');
	RETURN NEW;
END;
$$


CREATE TRIGGER DEL_Translator
BEFORE UPDATE  ON Translator
FOR EACH ROW 
EXECUTE PROCEDURE DEL_Translator();



CREATE FUNCTION UP_Is_translated()
RETURNS TRIGGER
language PLPGSQL
AS
$$
BEGIN
	INSERT INTO Is_translated_history values(OLD.* , CURRENT_DATE , 'update');
	RETURN NEW;
END;
$$


CREATE TRIGGER UP_Is_translated
BEFORE UPDATE  ON Is_translated
FOR EACH ROW 
EXECUTE PROCEDURE UP_Is_translated();



CREATE FUNCTION DEL_Is_translated()
RETURNS TRIGGER
language PLPGSQL
AS
$$
BEGIN
	INSERT INTO Is_translated_history values(OLD.* , CURRENT_DATE , 'delete');
	RETURN NEW;
END;
$$


CREATE TRIGGER DEL_Is_translated
BEFORE UPDATE  ON Is_translated
FOR EACH ROW 
EXECUTE PROCEDURE DEL_Is_translated();




CREATE FUNCTION UP_Library_member()
RETURNS TRIGGER
language PLPGSQL
AS
$$
BEGIN
	INSERT INTO Library_member_history values(OLD.* , CURRENT_DATE , 'update');
	RETURN NEW;
END;
$$


CREATE TRIGGER UP_Library_member
BEFORE UPDATE  ON Library_member
FOR EACH ROW 
EXECUTE PROCEDURE UP_Library_member;




CREATE FUNCTION DEL_Library_member()
RETURNS TRIGGER
language PLPGSQL
AS
$$
BEGIN
	INSERT INTO Library_member_history values(OLD.* , CURRENT_DATE , 'delete');
	RETURN NEW;
END;
$$


CREATE TRIGGER DEL_Library_member
BEFORE UPDATE  ON Library_member
FOR EACH ROW 
EXECUTE PROCEDURE DEL_Library_member;


CREATE FUNCTION UP_Borrow
RETURNS TRIGGER
language PLPGSQL
AS
$$
BEGIN
	INSERT INTO Borrow_history values(OLD.* , CURRENT_DATE , 'update');
	RETURN NEW;
END;
$$


CREATE TRIGGER UP_Borrow
BEFORE UPDATE  ON Borrow
FOR EACH ROW 
EXECUTE PROCEDURE UP_Borrow;


CREATE FUNCTION DEL_Borrow
RETURNS TRIGGER
language PLPGSQL
AS
$$
BEGIN
	INSERT INTO Borrow_history values(OLD.* , CURRENT_DATE , 'delete');
	RETURN NEW;
END;
$$


CREATE TRIGGER DEL_Borrow
BEFORE UPDATE  ON Borrow
FOR EACH ROW 
EXECUTE PROCEDURE DEL_Borrow;
