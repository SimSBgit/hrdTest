show databases;

use hrdtest;

-- 1. 테이블 3개 생성
create table golfMember (
	MNo int AUTO_INCREMENT,
	MName VARCHAR(30) NOT NULL,
	Phone VARCHAR(13),
	JoinDate DATE NOT NULL,
	Grade CHAR(1),
	CONSTRAINT Pk_MNo PRIMARY KEY (MNo),
	CONSTRAINT U_Phone UNIQUE (Phone),
	CONSTRAINT Ch_Grade CHECK (Grade IN ('A', 'B', 'C'))
);

CREATE TABLE lesson (
	LNo INT AUTO_INCREMENT,
	MNo INT,
	Coach VARCHAR(30) NOT NULL,
	StartDate DATE NOT NULL,
	Fee INT,
	CONSTRAINT Pk_LNo PRIMARY KEY (LNo),
	CONSTRAINT Fk_MNo FOREIGN KEY (MNo) REFERENCES golfMember(MNo) ON DELETE CASCADE,
	CONSTRAINT Ch_Fee CHECK (Fee >= 0)
);

CREATE TABLE usages (
	UNo INT AUTO_INCREMENT,
	MNo INT,
	UDate DATE NOT NULL,
	UsageTime INT,
	Cost INT,
	CONSTRAINT Pk_UNo PRIMARY KEY (UNo),
	CONSTRAINT Fk_MNo2 FOREIGN KEY (MNo) REFERENCES golfMember(MNo) ON DELETE CASCADE,
	CONSTRAINT Ch_UsageTime CHECK (UsageTime > 0),
	CONSTRAINT Ch_Cost CHECK (Cost >= 0)
);

DESC golfMember;
DESC lesson;
DESC usages;

DROP table if exists lesson;
DROP table if exists usages;
DROP table if exists golfMember;

-- 2. 각 테이블에 데이터 삽입
INSERT INTO golfMember (MName, Phone, JoinDate, Grade)
VALUES 
('홍길동', '010-1111-2222', '2020-01-01', 'A'),
('이순신', '010-3333-4444', '2021-05-10', 'B'),
('강감찬', '010-5555-6666', '2022-03-15', 'C');

INSERT INTO lesson (MNo, Coach, StartDate, Fee)
VALUES 
(1, '김프로', '2020-02-01', 300000),
(2, '박프로', '2021-06-01', 250000),
(3, '이프로', '2022-04-01', 200000);

INSERT INTO usages (MNo, UDate, UsageTime, Cost)
VALUES 
(1, '2020-02-10', 120, 24000),
(2, '2021-06-15', 90, 18000),
(3, '2022-04-20', 150, 30000);

SELECT * from lesson;
SELECT * from usages;
SELECT * from golfMember;

-- 3-1 A등급 회원의 이름, 전화번호, 가입일자 조회
SELECT MName, Phone, JoinDate FROM golfMember WHERE Grade = 'A';

-- 3-2 강습비가 250000 이상인 강습 내역 조회
SELECT LNo FROM lesson WHERE Fee >= 250000;

-- 3-3 회원별 총 이용요금 집계(출력: 회원명, 총 요금)
SELECT g.MName, COALESCE(SUM(u.Cost), 0) `총 요금` 
FROM golfMember g LEFT JOIN usages u ON g.MNo=u.MNo
GROUP BY g.MNo, g.MName;

-- 4. 이순신 회원의 등급을 A로 수정
UPDATE golfMember SET grade = 'A' WHERE MName = '이순신' AND MNo = 2;

-- 5. MNO = 3인 회원을 삭제
-- ON DELETE CASCADE / ON DELETE set null 일때
DELETE FROM golfMember WHERE MNo = 3; 

-- ON DELETE CASCADE가 아닐 때
DELETE FROM lesson WHERE MNo = 3; 
DELETE FROM usages WHERE MNo = 3;
DELETE FROM golfMember WHERE MNo = 3;

-- 6. 확장문제: 등급별 통계 - (등급, 회원수, 평균 강습비, 총 이용요금 조회)
SELECT g.Grade `회원등급`, 
	COUNT(g.MNo) `회원수`, 
	AVG(l.Fee) `평균 강습비`,
	SUM(u.TotalCost) `총 이용요금`
FROM golfMember g 
LEFT JOIN (SELECT MNo, AVG(Fee) AS Fee FROM lesson GROUP BY MNo)l ON g.MNo=l.MNo
LEFT JOIN (SELECT MNo, SUM(Cost) AS TotalCost FROM usages GROUP BY MNo)u ON g.MNo=u.MNo
GROUP BY g.Grade;