show databases;

USE hrdtest;

CREATE TABLE employee(
	EmpNo INT AUTO_INCREMENT PRIMARY KEY,
	EmpName VARCHAR(30) NOT NULL,
	Dept VARCHAR(20) NOT NULL,
	HireDate DATE NOT NULL,
	Salary INT,
	CHECK (Salary >= 2000000),
	CONSTRAINT U_Name UNIQUE (EmpName)
);

DESC employee;
SELECT * FROM employee;

DROP TABLE employee;

-- 1. 데이터 삽입하기.
INSERT INTO employee (EmpNo, EmpName, Dept, HireDate, Salary)
VALUES
(NULL, '홍길동', '영업부', '2020-03-01', 2500000),
(NULL, '이순신', '인사부', '2019-07-15', 3200000),
(NULL, '강감찬', '개발부', '2021-01-10', 2800000);

-- 2-1 부서(dept)가 개발부인 사원의 사번(empno), 이름(empname), 급여(salary) 조회
SELECT EmpNo, EmpName, Salary FROM employee WHERE Dept = '개발부';

-- 2-2 급여(salary)가 300만원 이상인 사원의 이름(empname)과 부서(dept) 조회
SELECT EmpName, Dept FROM employee WHERE Salary >= 3000000;

-- 3. 이순신의 급여(salary)를 350만원으로 수정.
UPDATE employee SET Salary = '3500000' WHERE EmpName = '이순신';

-- 4. 사번(empno)이 1번인 사원의 정보를 삭제.
DELETE FROM employee WHERE EmpNo = '1';






