package com.hrd.crud;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.hrd.conn.DBConnection;

public class CRUDClass {

	public CRUDClass() {

		Ex1(2020);  // 2020년부터 출판된 도서목록
		
		Ex2("홍길동");  // 홍길동 회원이 대출한 도서 목록
		
		Ex3();	// 반납하지 않은 도서 목록
		
		Ex4();	// 도서별 대출 횟수
//		
		Ex5();	// 가장 비싼 도서
		
	}

	
	private void Ex1(int year) {
		String sql = "SELECT BookTitle as `2020년부터 출판된 도서` FROM bookTable WHERE YEAR(PublishYear) >= ?";
		Connection conn = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		
		try {
			conn = DBConnection.getConnection();
			pstmt = conn.prepareStatement(sql);
			pstmt.setInt(1, year);
			
			rs = pstmt.executeQuery();
			
			System.out.println(year + "년부터 출판된 도서 목록");
			while(rs.next()) {
				String title = rs.getString("2020년부터 출판된 도서");
				System.out.println(" - " + title);
			}
		} catch (SQLException se) {
			se.printStackTrace();
			System.out.println(year + "년부터 출판된 도서 조회 실패");
		} finally {
			DBConnection.close(rs, pstmt, conn);
			System.out.println(year + "년부터 출판된 도서 조회 종료");
			System.out.println();
		}
	}

	private void Ex2(String name) {
		String sql = "SELECT b.BookTitle `홍길동 대출도서 목록`\r\n"
				+ "FROM rentalTable r INNER JOIN bookTable b ON r.BookID=b.BookID\r\n"
				+ "INNER JOIN memberTable m ON r.memberID=m.memberID \r\n"
				+ "WHERE m.NAME = ?";
		Connection conn = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		
		try {
			conn = DBConnection.getConnection();
			pstmt = conn.prepareStatement(sql);
			pstmt.setString(1, name);
			
			rs = pstmt.executeQuery();
			
			System.out.println(name + " 회원이 대출한 도서 목록");
			while(rs.next()) {
				String title = rs.getString("홍길동 대출도서 목록");
				System.out.println(" - " + title);
			}
		} catch (SQLException se) {
			se.printStackTrace();
			System.out.println(name + " 회원이 대출한 도서 조회 실패");
		} finally {
			DBConnection.close(rs, pstmt, conn);
			System.out.println(name + " 회원이 대출한 도서 조회 종료");
			System.out.println();
		}
	}

	private void Ex3() {
		String sql = "SELECT b.BookTitle `미반납도서` FROM bookTable b \r\n"
				+ "INNER JOIN rentalTable r ON b.BookID=r.BookID\r\n"
				+ "WHERE r.ReturnDate IS NULL \r\n"
				+ "GROUP BY b.BookID, b.BookTitle";
		Connection conn = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		
		try {
			conn = DBConnection.getConnection();
			pstmt = conn.prepareStatement(sql);
			rs = pstmt.executeQuery();
			
			System.out.println("반납하지 않은 도서 목록");
			while(rs.next()) {
				String title = rs.getString("미반납도서");
				System.out.println(" - " + title);
			}
		} catch (SQLException se) {
			se.printStackTrace();
			System.out.println("반납하지 않은 도서 목록 조회 실패");
		} finally {
			DBConnection.close(rs, pstmt, conn);
			System.out.println("반납하지 않은 도서 목록 조회 종료");
			System.out.println();
		}
	}

	private void Ex4() {
		String sql = "SELECT b.BookTitle `도서명`, COUNT(r.RentalID) AS `대출 횟수` \r\n"
				+ "FROM booktable b LEFT join rentaltable r ON r.BookID=b.BookID \r\n"
				+ "group BY b.BookID, b.BookTitle ORDER BY b.BookID;";
		Connection conn = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		
		try {
			conn = DBConnection.getConnection();
			pstmt = conn.prepareStatement(sql);			
			rs = pstmt.executeQuery();
			
			System.out.println("도서별 대출 횟수");
			while(rs.next()) {
				String title = rs.getString("도서명");
				String num = rs.getString("대출 횟수");
				System.out.println(" - " + title + ": " + num);
			}
			System.out.println("도서별 대출 횟수 조회 성공");
		} catch (SQLException se) {
			se.printStackTrace();
			System.out.println("도서별 대출 횟수 조회 실패");
		} finally {
			DBConnection.close(rs, pstmt, conn);
			System.out.println("도서별 대출 횟수 조회 종료");
			System.out.println();
		}
	}

	private void Ex5() {
		String sql = "SELECT BookTitle `가장 비싼 도서` FROM bookTable \r\n"
				+ "WHERE price = (SELECT MAX(price) FROM bookTable)";
		Connection conn = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		
		try {
			conn = DBConnection.getConnection();
			pstmt = conn.prepareStatement(sql);
			rs = pstmt.executeQuery();
			
			System.out.println("가장 비싼 도서");
			while(rs.next()) {
				String title = rs.getString("가장 비싼 도서");
				System.out.println(" - " + title);
			}
		} catch (SQLException se) {
			se.printStackTrace();
			System.out.println("가장 비싼 도서 조회 실패");
		} finally {
			DBConnection.close(rs, pstmt, conn);
			System.out.println("가장 비싼 도서 조회 종료");
			System.out.println();
		}
	}
	
}
