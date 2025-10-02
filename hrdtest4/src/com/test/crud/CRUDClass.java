package com.test.crud;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import com.test.conn.DBConnection;

public class CRUDClass {

	public CRUDClass() {
		
		Ex1();  // 두 테이블 생성
		
		Ex2_1("artist");  // 두 테이블에 인서트
		Ex2_2("album");
		
		Ex3_1("K-POP");	// 장르가 'k-pop'인 아티스트의 이름과 소속사 조회
		
		Ex3_2(1000000);	// 판매량이 100만 이상인 앨범의 제목과 판매량 조회
		
		Ex3_3();	// 아티스트별 총 판매량(출력: 아티스트명, 총 판매량)
		
		Ex4("아이유", "카카오엔터테인먼트");	// 아이유 소속사를 카카오엔터테인먼트로 수정
		
		Ex5(1);	// AlbumNo = 1인 앨범 삭제
		
		Ex6();	// 심화문제: 소속사별 매출현황(소속사, 아티스트 수, 총판매량 조회)
	}
	
	private void Ex1() {
		String sql1 = "CREATE TABLE artist(\r\n"
				+ "	ArtistNo INT AUTO_INCREMENT,\r\n"
				+ "	ArtistName VARCHAR(30) NOT NULL,\r\n"
				+ "	DebutDate DATE NOT NULL,\r\n"
				+ "	Genre VARCHAR(20) NOT NULL,\r\n"
				+ "	Agency VARCHAR(30),\r\n"
				+ "	CONSTRAINT Pk_ArtistNo PRIMARY KEY(ArtistNo),\r\n"
				+ "	CONSTRAINT U_ArtistName UNIQUE (ArtistName)\r\n"
				+ ")";
		
		String sql2 = "CREATE TABLE album(\r\n"
				+ "	AlbumNo INT AUTO_INCREMENT,\r\n"
				+ "	ArtistNo INT,\r\n"
				+ "	AlbumTitle VARCHAR(50) NOT NULL,\r\n"
				+ "	ReleaseDate DATE NOT NULL,\r\n"
				+ "	Sales INT,\r\n"
				+ "	CONSTRAINT Pk_AlbumNo PRIMARY KEY (AlbumNo),\r\n"
				+ "	CONSTRAINT Fk_ArtistNo FOREIGN KEY (ArtistNo) REFERENCES artist(ArtistNo),\r\n"
				+ "	CONSTRAINT CH_ CHECK (Sales >= 0)\r\n"
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
		String sql = "INSERT INTO " + tableName +" (ArtistName, DebutDate, Genre, Agency)\r\n"
				+ "VALUES\r\n"
				+ "('아이유', '2008-09-18', '발라드', 'EDAM엔터테인먼트'),\r\n"
				+ "('BTS', '2013-06-13', 'K-POP', '하이브'),\r\n"
				+ "('블랙핑크', '2016-08-08', 'K-POP', 'YG엔터테인먼트')";
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
	
	private void Ex2_2(String tableName) {
		String sql = "INSERT INTO "+ tableName +" (ArtistNo, AlbumTitle, ReleaseDate, Sales)\r\n"
				+ "VALUES\r\n"
				+ "(1, '좋은 날', '2010-12-09', 500000),\r\n"
				+ "(2, 'MAP OF THE SOUL: 7', '2020-02-21', 4300000),\r\n"
				+ "(3, 'THE ALBUM', '2020-10-02', 1300000)";
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

	private void Ex3_1(String genre) {
		String sql = "SELECT ArtistName, Agency FROM artist WHERE Genre = ?";
		Connection conn = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		
		try {
			conn = DBConnection.getConnection();
			pstmt = conn.prepareStatement(sql);
			pstmt.setString(1, genre);
			rs = pstmt.executeQuery();
			
			System.out.println(genre +" 아티스트 조회");
			while(rs.next()) {
				String aName = rs.getString("ArtistName");
				String agency = rs.getString("Agency");
				System.out.println(" - 활동명: " + aName + ", 소속사: " 
				+ agency);
			}
		} catch (SQLException se) {
			se.printStackTrace();
			System.out.println(genre +" 아티스트 조회 실패");
		} finally {
			DBConnection.close(rs, pstmt, conn);
			System.out.println(genre +" 아티스트 조회 완료");
			System.out.println();
		}
	}
	
	private void Ex3_2(int sales) {
		String sql = "SELECT AlbumTitle, Sales FROM album WHERE Sales >= ?";
		Connection conn = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		
		try {
			conn = DBConnection.getConnection();
			pstmt = conn.prepareStatement(sql);
			pstmt.setInt(1, sales);
			rs = pstmt.executeQuery();
			
			System.out.println("판매량이 "+ sales +" 이상인 앨범 조회");
			while(rs.next()) {
				String title = rs.getString("AlbumTitle");
				int selling = rs.getInt("Sales");
				System.out.println(" - 앨범명: " + title + ", 판매량: " 
				+ selling);
			}
		} catch (SQLException se) {
			se.printStackTrace();
			System.out.println("판매량이 "+ sales +" 이상인 앨범 조회 실패");
		} finally {
			DBConnection.close(rs, pstmt, conn);
			System.out.println("판매량이 "+ sales +" 이상인 앨범 조회 완료");
			System.out.println();
		}
	}
	
	private void Ex3_3() {
		String sql = "SELECT ArtistName, COALESCE(SUM(Sales), 0) `총 판매량`\r\n"
				+ "FROM artist ar LEFT JOIN album al ON ar.ArtistNo=al.ArtistNo\r\n"
				+ "GROUP BY ar.ArtistNo, ar.ArtistName;";
		Connection conn = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		
		try {
			conn = DBConnection.getConnection();
			pstmt = conn.prepareStatement(sql);
			rs = pstmt.executeQuery();
			
			System.out.println("아티스트별 총 판매량");
			while(rs.next()) {
				String aName = rs.getString("ArtistName");
				int total = rs.getInt("총 판매량");
				System.out.println(" - 활동명: " + aName + ", 총 판매량: " 
				+ total);
			}
		} catch (SQLException se) {
			se.printStackTrace();
			System.out.println("아티스트별 총 판매량 조회 실패");
		} finally {
			DBConnection.close(rs, pstmt, conn);
			System.out.println("아티스트별 총 판매량 조회 완료");
			System.out.println();
		}
	}
	
	private void Ex4(String name, String agency) {
		String sql = "UPDATE artist SET Agency = ? WHERE ArtistName = ?";
		Connection conn = null;
		PreparedStatement pstmt = null;
		
		try {
			conn = DBConnection.getConnection();
			pstmt = conn.prepareStatement(sql);
			
			pstmt.setString(1, agency);
			pstmt.setString(2, name);
			
			int rows = pstmt.executeUpdate();
			
			System.out.println(name +" 소속사를 "+ agency +"로 수정");
			if (rows > 0) {
				System.out.println(" --- " + rows + "행 수정 완료");
			} else {
				System.out.println("변경사항이 없습니다.");
			}
		} catch (SQLException se) {
			se.printStackTrace();
			System.out.println("아이유 소속사를 카카오엔터테인먼트로 수정 실패");
		} finally {
			DBConnection.close(pstmt, conn);
			System.out.println("아이유 소속사를 카카오엔터테인먼트로 수정 완료");
			System.out.println();
		}
	}
	
	private void Ex5(int albumNum) {
		String sql = "DELETE FROM album WHERE AlbumNo = ?";
		Connection conn = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		
		try {
			conn = DBConnection.getConnection();
			pstmt = conn.prepareStatement(sql);
			
			pstmt.setInt(1, albumNum);
			
			int rows = pstmt.executeUpdate();
			
			System.out.println("AlbumNo = "+ albumNum +"인 앨범 삭제");
			if (rows > 0) {
				System.out.println(" --- " + rows + "행 삭제 완료");
			} else {
				System.out.println("변경사항이 없습니다.");
			}
		} catch (SQLException se) {
			se.printStackTrace();
			System.out.println("AlbumNo = "+ albumNum +"인 앨범 삭제 실패");
		} finally {
			DBConnection.close(rs, pstmt, conn);
			System.out.println("AlbumNo = "+ albumNum +"인 앨범 삭제 완료");
			System.out.println();
		}
	}
	
	private void Ex6() {
		String sql = "SELECT ar.Agency `소속사`, \r\n"
				+ "	COUNT(DISTINCT ar.ArtistNo) `아티스트 수`, \r\n"
				+ "	COALESCE(SUM(al.Sales), 0) `총 판매량` \r\n"
				+ "FROM artist ar LEFT JOIN album al ON ar.ArtistNo=al.ArtistNo \r\n"
				+ "GROUP BY ar.Agency";
		Connection conn = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		
		try {
			conn = DBConnection.getConnection();
			pstmt = conn.prepareStatement(sql);
			rs = pstmt.executeQuery();
			
			System.out.println("소속사별 매출 현황");
			while(rs.next()) {
				int totalNum = rs.getInt("아티스트 수");
				String agency = rs.getString("소속사");
				int totalSale = rs.getInt("총 판매량");
				System.out.println(" - 소속사: " + agency + ", 아티스트 수: " 
				+ totalNum + ", 총 판매량: " + totalSale);
			}
		} catch (SQLException se) {
			se.printStackTrace();
			System.out.println("소속사별 매출 현황 조회 실패");
		} finally {
			DBConnection.close(rs, pstmt, conn);
			System.out.println("소속사별 매출 현황 조회 완료");
			System.out.println();
		}
	}
	
}
