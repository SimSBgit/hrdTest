package com.test3.crud;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import com.test3.conn.DBConnection;

public class CRUDClass {


	public CRUDClass() {
		
		Ex1();  // 두 테이블 생성
		
		Ex2_1("shopmember");  // shopmember 테이블에 인서트
		
		Ex2_2_1("A");	// 급여(salary)가 300만원 이상인 사원의 이름(empname)과 부서(dept) 조회.
		
		Ex2_2_2(2020);	// 가입일자가 2020년 이후인 회원 조회
		
		Ex3("sale");	// sale 테이블에 인서트
		
		Ex4_1();		// 회원별 총 구매금액(회원번호, 회원성명, 총 구매금액 조회)
		
		Ex4_2();		// 가장 구매금액이 높은 회원의 이름과 금액 조회
		
		Ex5_1("이순신", "A");	// 이순신 회원의 등급을 A로 수정
		
		Ex5_2(3);		// CustNo = 3인 회원을 삭제
	}
	
	private void Ex1() {
		String sql1 = "CREATE TABLE if not exists shopmember (\r\n"
				+ "	CustNo int AUTO_INCREMENT,\r\n"
				+ "	CustName VARCHAR(30) NOT NULL,\r\n"
				+ "	Phone VARCHAR(13),\r\n"
				+ "	Address VARCHAR(50),\r\n"
				+ "	JoinDate DATE NOT NULL,\r\n"
				+ "	Grade CHAR(1),\r\n"
				+ "	CONSTRAINT Pk_CustNo PRIMARY KEY (CustNo),\r\n"
				+ "	CONSTRAINT U_Address UNIQUE (Address),\r\n"
				+ "	CHECK (Grade IN ('A', 'B', 'C'))\r\n"
				+ ")";
		
		String sql2 = "CREATE TABLE if not exists sale (\r\n"
				+ "	SaleNo INT AUTO_INCREMENT,\r\n"
				+ "	CustNo INT,\r\n"
				+ "	PCost INT,\r\n"
				+ "	Amount INT,\r\n"
				+ "	Price INT,\r\n"
				+ "	PCode CHAR(3),\r\n"
				+ "	CONSTRAINT Pk_SaleNo PRIMARY KEY (SaleNo),\r\n"
				+ "	CONSTRAINT Fk_CustNo FOREIGN KEY (CustNo) \r\n"
				+ "	REFERENCES shopmember(CustNo) ON DELETE CASCADE ON UPDATE CASCADE\r\n"
				+ ")";
		
		Connection conn = null;
		Statement stmt = null;
		
		try {
			conn = DBConnection.getConnection();
			stmt = conn.createStatement();
			stmt.execute(sql1);
			stmt.execute(sql2);
			
			System.out.println("테이블 생성 성공");
		} catch (SQLException se) {
			se.printStackTrace();
			System.out.println("테이블 생성 실패");
		} finally {
			DBConnection.close(stmt, conn);
			System.out.println("테이블 생성 종료");
			System.out.println();
		}
	}

	private void Ex2_1(String tableName) {
		String sql = "INSERT INTO " + tableName +" (CustName, Phone, Address, JoinDate, Grade)\r\n"
				+ "VALUES\r\n"
				+ "('홍길동', '010-1234-5678', '서울시 강남구', '2020-01-01', 'A'),\r\n"
				+ "('이순신', '010-2222-3333', '부산시 해운대구', '2021-03-15', 'B'),\r\n"
				+ "('강감찬', '010-7777-8888', '대구시 달서구', '2019-05-20', 'C')";
		Connection conn = null;
		Statement stmt = null;
		
		try {
			conn = DBConnection.getConnection();
			stmt = conn.createStatement();
			stmt.executeUpdate(sql);
			
			System.out.println(tableName + "에 인서트 완료");
		} catch (SQLException se) {
			se.printStackTrace();
			System.out.println(tableName + "에 인서트 실패");
		} finally {
			DBConnection.close(stmt, conn);
			System.out.println("인서트 종료");
			System.out.println();
		}
	}

	private void Ex2_2_1(String grade) {
		String sql = "SELECT CustName `이름`, Phone `전화번호`, JoinDate `가입일자`\r\n"
				+ "FROM shopmember WHERE Grade = ?";
		Connection conn = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		
		try {
			conn = DBConnection.getConnection();
			pstmt = conn.prepareStatement(sql);
			pstmt.setString(1, grade);
			rs = pstmt.executeQuery();
			
			System.out.println("고객등급이 " + grade + "등급인 회원 조회");
			while(rs.next()) {
				String emName = rs.getString("이름");
				String phone = rs.getString("전화번호");
				Date joinDate = rs.getDate("가입일자");
				System.out.println(" - 이름: " + emName + ", 전화번호: " 
				+ phone + ", 가입일자: " + joinDate);
			}
		} catch (SQLException se) {
			se.printStackTrace();
			System.out.println("회원 조회 실패");
		} finally {
			DBConnection.close(rs, pstmt, conn);
			System.out.println("회원 조회 완료");
			System.out.println();
		}
	}
	
	private void Ex2_2_2(int year) {
		String sql = "SELECT CustName `이름` FROM shopmember WHERE JoinDate > ?";
		Connection conn = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		
		try {
			conn = DBConnection.getConnection();
			pstmt = conn.prepareStatement(sql);
			
			String dataStr = year + "-12-31";
			pstmt.setDate(1, java.sql.Date.valueOf(dataStr));
			
			rs = pstmt.executeQuery();
			
			System.out.println("가입일자가 " + year + "년 이후인 회원 조회");
			while(rs.next()) {
				String emName = rs.getString("이름");
				System.out.println(" - 이름: " + emName);
			}
		} catch (SQLException se) {
			se.printStackTrace();
			System.out.println("가입일자 조회 실패");
		} finally {
			DBConnection.close(rs, pstmt, conn);
			System.out.println("가입일자 조회 완료");
			System.out.println();
		}
	}
	
	private void Ex3(String tableName) {
		String sql = "INSERT INTO sale (CustNo, PCost, Amount, Price, PCode)\r\n"
				+ "VALUES\r\n"
				+ "(1, 1000, 10, 10000, 'P01'),\r\n"
				+ "(2, 2000, 5, 10000, 'P02'),\r\n"
				+ "(3, 1500, 7, 10500, 'P03')";
		Connection conn = null;
		Statement stmt = null;
		
		try {
			conn = DBConnection.getConnection();
			stmt = conn.createStatement();
			stmt.executeUpdate(sql);
			
			System.out.println(tableName + "에 인서트 완료");
		} catch (SQLException se) {
			se.printStackTrace();
			System.out.println(tableName + "에 인서트 실패");
		} finally {
			DBConnection.close(stmt, conn);
			System.out.println("인서트 종료");
			System.out.println();
		}
	}
	
	private void Ex4_1() {
		String sql = "SELECT sh.CustNo `회원번호`, sh.CustName `회원성명`, COALESCE(SUM(sa.Price),0) `총 구매금액`\r\n"
				+ "FROM shopmember sh LEFT JOIN sale sa ON sh.CustNo=sa.CustNo\r\n"
				+ "GROUP BY sh.CustNo, sh.CustName";
		Connection conn = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		
		try {
			conn = DBConnection.getConnection();
			pstmt = conn.prepareStatement(sql);
			rs = pstmt.executeQuery();
			
			System.out.println("회원별 총 구매금액 조회");
			while(rs.next()) {
				String custName = rs.getString("회원성명");
				int custNo = rs.getInt("회원번호");
				int totalPrice = rs.getInt("총 구매금액");
				System.out.println(" - 회원성명: " + custName + ", 회원번호: " 
				+ custNo + ", 총 구매금액: " + totalPrice);
			}
		} catch (SQLException se) {
			se.printStackTrace();
			System.out.println("구매금액 조회 실패");
		} finally {
			DBConnection.close(rs, pstmt, conn);
			System.out.println("구매금액 조회 완료");
			System.out.println();
		}
	}
	
	private void Ex4_2() {
		String sql = "SELECT sh.CustName `회원성명`, SUM(sa.Price) `총 구매금액`\r\n"
				+ "FROM shopmember sh INNER JOIN sale sa ON sh.CustNo=sa.CustNo\r\n"
				+ "GROUP BY sh.CustNo, sh.CustName HAVING SUM(sa.Price) = (\r\n"
				+ "SELECT MAX(t.total) FROM (SELECT SUM(Price) AS total FROM sale GROUP BY CustNo)t)";
		Connection conn = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		
		try {
			conn = DBConnection.getConnection();
			pstmt = conn.prepareStatement(sql);
			rs = pstmt.executeQuery();
			
			System.out.println("가장 구매금액이 높은 회원 조회");
			while(rs.next()) {
				String custName = rs.getString("회원성명");
				int totalPrice = rs.getInt("총 구매금액");
				System.out.println(" - 회원성명: " + custName + ", 총 구매금액: " + totalPrice);
			}
		} catch (SQLException se) {
			se.printStackTrace();
			System.out.println("가장 구매금액이 높은 회원 조회 실패");
		} finally {
			DBConnection.close(rs, pstmt, conn);
			System.out.println("가장 구매금액이 높은 회원 조회 완료");
			System.out.println();
		}
	}

	private void Ex5_1(String name ,String grade) {
		String sql = "UPDATE shopmember SET Grade = ? WHERE CustName = ?";
		Connection conn = null;
		PreparedStatement pstmt = null;
		
		try {
			conn = DBConnection.getConnection();
			pstmt = conn.prepareStatement(sql);
			pstmt.setString(1, grade);
			pstmt.setString(2, name);
			int rows = pstmt.executeUpdate();
			
			if (rows > 0) {
				System.out.println(rows + "행 수정 완료");
			} else {
				System.out.println("수정할 사항이 없습니다.");
			}
			System.out.println(name + " 회원의 등급을 " + grade + "로 수정");
		} catch (SQLException se) {
			se.printStackTrace();
			System.out.println("등급 수정 실패");
		} finally {
			DBConnection.close(pstmt, conn);
			System.out.println("등급 수정 완료");
			System.out.println();
		}
	}
	
	private void Ex5_2(int custNo) {
		String sql = "DELETE FROM shopmember WHERE CustNo = ?";
		Connection conn = null;
		PreparedStatement pstmt = null;
		
		try {
			conn = DBConnection.getConnection();
			pstmt = conn.prepareStatement(sql);
			pstmt.setInt(1, custNo);
			int rows = pstmt.executeUpdate();
			
			if (rows > 0) {
				System.out.println(custNo + "행에 위치한 회원을 삭제하겠습니다.");
			} else {
				System.out.println("삭제할 사항이 없습니다.");
			}
			System.out.println("회원번호가 " + custNo + "인 회원 삭제");
		} catch (SQLException se) {
			se.printStackTrace();
			System.out.println("회원 삭제 실패");
		} finally {
			DBConnection.close(pstmt, conn);
			System.out.println("회원 삭제 완료");
			System.out.println();
		}
	}
}
