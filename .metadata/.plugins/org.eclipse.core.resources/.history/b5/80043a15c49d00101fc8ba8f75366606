USE hrdtest;

DESC shopmember;
DESC sale;

DROP TABLE shopmember;
DROP TABLE sale;

-- 1. 테이블 생성
CREATE TABLE shopmember (
	CustNo int AUTO_INCREMENT,
	CustName VARCHAR(30) NOT NULL,
	Phone VARCHAR(13),
	Address VARCHAR(50),
	JoinDate DATE NOT NULL,
	Grade CHAR(1),
	CONSTRAINT Pk_CustNo PRIMARY KEY (CustNo),
	CONSTRAINT U_Address UNIQUE (Address),
	CHECK (Grade IN ('A', 'B', 'C'))
);

CREATE TABLE sale (
	SaleNo INT AUTO_INCREMENT,
	CustNo INT,
	PCost INT,
	Amount INT,
	Price INT,
	PCode CHAR(3),
	CONSTRAINT Pk_SaleNo PRIMARY KEY (SaleNo),
	CONSTRAINT Fk_CustNo FOREIGN KEY (CustNo) 
	REFERENCES shopmember(CustNo) ON DELETE CASCADE ON UPDATE CASCADE
);

-- 2-1 데이터 삽입
INSERT INTO shopmember (CustName, Phone, Address, JoinDate, Grade)
VALUES
('홍길동', '010-1234-5678', '서울시 강남구', '2020-01-01', 'A'),
('이순신', '010-2222-3333', '부산시 해운대구', '2021-03-15', 'B'),
('강감찬', '010-7777-8888', '대구시 달서구', '2019-05-20', 'C');

SELECT * FROM shopmember;

-- 2-2-1 고객등급이 A등급인 회원의 이름, 전화번호, 가입일자 조회
SELECT CustName `이름`, Phone `전화번호`, JoinDate `가입일자`
FROM shopmember WHERE Grade = 'A';

-- 2-2-2 가입일자가 2020년 이후인 회원 조회
SELECT CustName `이름` FROM shopmember WHERE DATE(JoinDate) > '2020-12-31';

-- 3. 데이터 삽입
INSERT INTO sale (CustNo, PCost, Amount, Price, PCode)
VALUES
(1, 1000, 10, 10000, 'P01'),
(2, 2000, 5, 10000, 'P02'),
(3, 1500, 7, 10500, 'P03');

SELECT * FROM sale;

-- 4-1 회원별 총 구매금액(회원번호, 회원성명, 총 구매금액 조회)
SELECT sa.CustNo `회원번호`, sh.CustName `회원성명`, sa.price `총 구매금액`
FROM shopmember sh LEFT JOIN sale sa ON sh.CustNo=sa.CustNo
GROUP BY sh.CustNo, sh.CustName;

-- 4-2 가장 구매금액이 높은 회원의 이름과 금액 조회
-- order by
SELECT sh.CustName `회원성명`, SUM(sa.Price) `총 구매금액`
FROM shopmember sh INNER JOIN sale sa ON sh.CustNo=sa.CustNo
GROUP BY sh.CustNo, sh.CustName ORDER BY `총 구매금액` DESC LIMIT 1;

-- having
SELECT sh.CustName `회원성명`, SUM(sa.Price) `총 구매금액`
FROM shopmember sh INNER JOIN sale sa ON sh.CustNo=sa.CustNo
GROUP BY sh.CustNo, sh.CustName HAVING SUM(sa.Price) = (
SELECT MAX(t.total) FROM (SELECT SUM(Price) AS total FROM sale GROUP BY CustNo)t);

-- 5-1 이순신 회원의 등급을 A로 수정
UPDATE shopmember SET Grade = 'A' WHERE CustName = '이순신';

-- 5-2 CustNo = 3인 회원을 삭제
DELETE FROM shopmember WHERE CustNo = 3;
