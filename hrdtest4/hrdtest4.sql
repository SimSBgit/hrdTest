show databases;

use hrdtest;

-- 1. 테이블 생성
CREATE TABLE artist(
	ArtistNo INT AUTO_INCREMENT,
	ArtistName VARCHAR(30) NOT NULL,
	DebutDate DATE NOT NULL,
	Genre VARCHAR(20) NOT NULL,
	Agency VARCHAR(30),
	CONSTRAINT Pk_ArtistNo PRIMARY KEY(ArtistNo),
	CONSTRAINT U_ArtistName UNIQUE (ArtistName)
);

CREATE TABLE album(
	AlbumNo INT AUTO_INCREMENT,
	ArtistNo INT,
	AlbumTitle VARCHAR(50) NOT NULL,
	ReleaseDate DATE NOT NULL,
	Sales INT,
	CONSTRAINT Pk_AlbumNo PRIMARY KEY (AlbumNo),
	CONSTRAINT Fk_ArtistNo FOREIGN KEY (ArtistNo) REFERENCES artist(ArtistNo),
	CONSTRAINT CH_ CHECK (Sales >= 0)
);

DROP TABLE album;
DROP TABLE artist;


DESC artist;
DESC album;

-- 2-1 아티스트 데이터 삽입
INSERT INTO artist (ArtistName, DebutDate, Genre, Agency)
values
('아이유', '2008-09-18', '발라드', 'EDAM엔터테인먼트'),
('BTS', '2013-06-13', 'K-POP', '하이브'),
('블랙핑크', '2016-08-08', 'K-POP', 'YG엔터테인먼트');

-- 2-2 앨범 데이터 삽입
INSERT INTO album (ArtistNo, AlbumTitle, ReleaseDate, Sales)
VALUES
(1, '좋은 날', '2010-12-09', 500000),
(2, 'MAP OF THE SOUL: 7', '2020-02-21', 4300000),
(3, 'THE ALBUM', '2020-10-02', 1300000);

SELECT * FROM artist;
SELECT * FROM album;

-- 3-1 장르가 'k-pop'인 아티스트의 이름과 소속사 조회
SELECT ArtistName, Agency FROM artist WHERE Genre = 'K-POP';

-- 3-2 판매량이 100만 이상인 앨범의 제목과 판매량 조회
SELECT AlbumTitle, Sales FROM album WHERE Sales >= 1000000;

-- 3-3 아티스트별 총 판매량(출력: 아티스트명, 총 판매량)
SELECT ArtistName, COALESCE(SUM(Sales), 0) `총 판매량`
FROM artist ar LEFT JOIN album al ON ar.ArtistNo=al.ArtistNo
GROUP BY ar.ArtistNo, ar.ArtistName;

-- 4. 아이유 소속사를 카카오엔터테인먼트로 수정
UPDATE artist SET Agency = '카카오엔터테인먼트' WHERE ArtistName = '아이유';

-- 5. AlbumNo = 1인 앨범 삭제
DELETE FROM album WHERE AlbumNo = 1;

-- 심화문제: 소속사별 매출현황(소속사, 아티스트 수, 총판매량 조회).
SELECT ar.Agency `소속사`, 
	COUNT(DISTINCT ar.ArtistNo) `아티스트 수`, 
	COALESCE(SUM(al.Sales), 0) `총 판매량` 
FROM artist ar LEFT JOIN album al ON ar.ArtistNo=al.ArtistNo 
GROUP BY ar.Agency;
