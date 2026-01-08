package main.java;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Scanner;

public class DataHandling {
	private void run() {
		// TODO Auto-generated method stub
		System.out.println("2026-01-07~ 진행중인 BigDataHandling 테스트");
		System.out.println("----------------------------------프로그램 시작--------------------------------------------");

	}
	public static void main(String[] args) {

		DataHandling dataHandling = new DataHandling();
		dataHandling.run();

		// 파일 읽기
		System.out.println();
		System.out.println("-----------------------------------파일 불러오기-------------------------------------------");
		File bigDataFile = new File("data/n.kor_4.dat");

		if (!bigDataFile.exists()) {
			System.out.println("해당 경로에 데이터 파일이 존재하지 않거나, 해당 경로가 잘못되었습니다.");
			return;
		}

		FileInputStream iStream = null;
		BufferedInputStream biStream = null;

		// 파일 읽기 (IOException 예외처리)
		try {
			iStream = new FileInputStream(bigDataFile);

			biStream = new BufferedInputStream(iStream);

			byte[] buffer = new byte[8192];

			// 기능 선택 && InputStream 시작
			System.out.println("----------------------------InputStream이 파일을 읽습니다----------------------------");
			int readBuffSize;
			Scanner sc = new Scanner(System.in);

			do {
				int SKLL = 0;
				System.out.println("----------------------------기능 선택----------------------------");
				System.out.println("1. 데이터 갯수 출력 						 	-> 출력하고 재 loop");
				System.out.println("2. 각 데이터 컬럼별 가장 긴 데이터의 크기 출력 	 	-> 출력하고 재 loop");
				System.out.println("3. 출원일자 컬럼 연도가 같은 데이터끼리 파일 분리 	-> 파일을 분리 생성하고 종료");
				System.out.println("---------------------------------------------------------------");
				System.out.printf("실행 할 기능 : ");
				SKLL = Integer.parseInt(sc.nextLine());

				/*
				 * 실행할 기능 번호 SKLL 에 따른 실행 함수 분리 1. 데이터 갯수 출력 2. 각 데이터 컬럼별 가장 긴 데이터의 크기 출력 3.
				 * 출원일자 컬럼 연도가 같은 데이터끼리 파일 분리 etc 입력. 종료
				 */

				if (SKLL == 1) {
					/*
					 * 1. 데이터 갯수 출력 -> 출력하고 재 loop
					 */
//					countData(biStream, buffer);

				} else if (SKLL == 2) {
					/*
					 * 2. 각 데이터 컬럼별 가장 긴 데이터의 크기 출력 -> 출력하고 재 loop
					 */
				} else if (SKLL == 3) {
					/*
					 * 3. 출원일자 컬럼 연도가 같은 데이터끼리 파일 분리 -> 파일을 분리해서 생성하고 종료
					 */
				}

				if (SKLL != 1 && SKLL != 2 && SKLL != 3) {
					System.out.println(
							"--------------------------------etc 입력을 받아 프로그램을 종료합니다--------------------------------");
					break;
				}

				// read()는 읽은 바이트 수를 반환
				readBuffSize = biStream.read(buffer);

				if (readBuffSize == -1) {
					break;
				}

				// 버퍼가 다 안찼는데 파일을 다 읽은경우 -> 그대로 채워진 만큼의 readBuffSize, 버퍼의 String 출력하고 끝내야함.
				if (readBuffSize < buffer.length) {
					System.out.println("--------------------------버퍼가 차지않았는데 파일을 다 읽었습니다--------------------------");
					System.out.println("-----------------------------------------채워진 버퍼 사이즈: " + readBuffSize
							+ "-----------------------------------------");
					System.out.println(new String(buffer, 0, readBuffSize, "UTF-8"));
					System.out.println();
					break;
				}

				// FIXME 풀 버퍼로 파일을 읽을경우 버퍼의 끝부분의 글자가 깨지는 문제가 남아있음 -> 버퍼 사이즈보다 약간 작게 읽어서 해결??
				// 버퍼가 다 찰때까지 파일을 읽은경우 -> 채워진 readBuffSize, 버퍼의 String을 출력.
				System.out.println("-----------------------------------------채워진 버퍼 사이즈: " + readBuffSize
						+ "------------------------------------------");
				System.out.println(new String(buffer, 0, readBuffSize, "UTF-8"));
				System.out.println();

				// 버퍼 비우기
				Arrays.fill(buffer, (byte) 0);
				System.out.println(
						"-----------------------------------------버퍼 비워짐-----------------------------------------");
				System.out.println();

			} while (true);
			System.out.println("------------------------------------------완료-----------------------------------------");

		} catch (IOException e) {
			System.out.println(e);
		} finally {
			try {
				biStream.close();
				iStream.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		System.out.println("---------------------------------------프로그램 종료---------------------------------------");
	}

	// BIStream
	// 데이터 갯수 파악 메소드 -> readLine() 사용, return ``0 카운팅횟수
	public void countData(BufferedInputStream biStream, byte[] buffer) throws IOException {
		System.out.println("--------------------------데이터 갯수 파악--------------------------");
		int readBuffSize;

		while(true) {
			//BufferedInputStream 에 readLine 없나?
			readBuffSize = biStream.read(buffer);

			// 종료 조건
			if(readBuffSize == -1) {
				System.out.println("---------------------------------------파일을 끝까지 읽었습니다---------------------------------------");
				System.out.println("데이터의 갯수 : ");
				return;
			}


		}
	}
}
