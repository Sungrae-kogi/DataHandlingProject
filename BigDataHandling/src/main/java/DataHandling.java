package main.java;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Queue;
import java.util.Scanner;
import java.util.Set;

import org.apache.commons.cli.Options;



public class DataHandling {
	private String inputFileName;

	// main
	public static void main(String[] args) {

		DataHandling dataHandling = new DataHandling();

		//
		Options options = new Options();



		dataHandling.withoutProperties();

		dataHandling.run();

	}

	private void run() {
		// TODO Auto-generated method stub
		System.out.println("2026-01-07~ 진행중인 BigDataHandling 테스트");
		System.out.println("----------------------------------프로그램 시작--------------------------------------------");

		// 기능 선택 && InputStream 시작
		Scanner sc = new Scanner(System.in);

		long beforeTime;
		long afterTime;
		long secDiffTime;

		do {
			String SKLL = "";
			System.out.println("-------------------------------------기능 선택-----------------------------------------");
			System.out.println("1. 데이터 갯수 출력 						 	-> 출력하고 재 loop");
			System.out.println("2. 각 데이터 컬럼별 가장 긴 데이터의 크기 출력 	 	-> 출력하고 재 loop");
			System.out.println("3. 출원일자 컬럼 연도가 같은 데이터끼리 파일 분리 	-> 파일을 분리 생성하고 종료");
			System.out.println("---------------------------------------------------------------------------------");
			System.out.printf("실행 할 기능 : ");
			SKLL = sc.nextLine();

			/*
			 * 실행할 기능 번호 SKLL 에 따른 실행 함수 분리 1. 데이터 갯수 출력 2. 각 데이터 컬럼별 가장 긴 데이터의 크기 출력 3.
			 * 출원일자 컬럼 연도가 같은 데이터끼리 파일 분리 etc 입력. 종료
			 */
			if (SKLL.equals("1")) {
				/*
				 * 1. 데이터 갯수 출력 -> 출력하고 재 loop
				 */
				// 코드 실행 전에 시간 받아오기
				beforeTime = System.currentTimeMillis();

				// 실행
				countData();

				// 실행 시간 (초)
				afterTime = System.currentTimeMillis();
				secDiffTime = (afterTime - beforeTime) / 1000;
				System.out.println("실행 시간(sec) : " + secDiffTime);

			} else if (SKLL.equals("2")) {
				/*
				 * 2. 각 데이터 컬럼별 가장 긴 데이터의 크기 출력 -> 출력하고 재 loop
				 */
				// 코드 실행 전에 시간 받아오기
				beforeTime = System.currentTimeMillis();

				// 실행
				findMaxDataSizePerColumn();

				// 실행 시간 (초)
				afterTime = System.currentTimeMillis();
				secDiffTime = (afterTime - beforeTime) / 1000;
				System.out.println("실행 시간(sec) : " + secDiffTime);

			} else if (SKLL.equals("3")) {
				/*
				 * 3. 출원일자 컬럼 연도가 같은 데이터끼리 파일 분리 -> 파일을 분리해서 생성하고 종료
				 */
				// 코드 실행 전에 시간 받아오기
				beforeTime = System.currentTimeMillis();

				// 실행
				divideFileByYear();

				// 실행 시간 (초)
				afterTime = System.currentTimeMillis();
				secDiffTime = (afterTime - beforeTime) / 1000;
				System.out.println("실행 시간(sec) : " + secDiffTime);

			} else {
				System.out.println(
						"--------------------------------etc 입력을 받아 프로그램을 종료합니다--------------------------------");
				break;
			}

		} while (true);
		System.out.println("------------------------------------------완료-----------------------------------------");

		System.out.println("---------------------------------------프로그램 종료---------------------------------------");

	}

	private FileReader createFileReader() throws IOException {
		// TODO Auto-generated method stub
		return new FileReader(inputFileName);
	}

	private FileWriter createFileWriter(String year) throws IOException {
		return new FileWriter("data/data_splitted" + year + ".dat");
	}

	private FileWriter reWriteFileWriter(String year, boolean isAppend) throws IOException {
		return new FileWriter("data/data_splitted" + year + ".dat", isAppend);
	}

	private void withoutProperties() {
		inputFileName = "data/n.kor.dat";
	}

	private void initProperties(String path) {
		// TODO Auto-generated method stub
		Properties prop = new Properties();

		try (InputStream iStream = new FileInputStream(path);) {
			prop.load(iStream);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			System.out.println("properties Exception");
			e.printStackTrace();
		}

		inputFileName = prop.getProperty("DATA_FILE");

	}

	// 대용량 데이터의 경우 메모리 이슈로 지속적으로 데이터를 들고 있을 수가없고, 있을 필요도 없다. 따라서 Collections가 처리에
	// 효율적이라 하더라도 그 효율적인것을 버리는 상황이 나옴.
	// readLine() && Stream 사용을 위해 BufferedReader 사용
	// 데이터 갯수 파악 메소드 -> readLine() 사용, return ``0 카운팅횟수
	public void countData() {
		System.out.println("-----------------------------------데이터 갯수 파악---------------------------------------");
		int count = 0;

		try (BufferedReader bReader = new BufferedReader(createFileReader());) {
			System.out
					.println("--------------------------------------파일 불러오기-----------------------------------------");
			String str = "";

			while (true) {
				// 종료 조건 -> BufferedReader는 읽을 데이터가 없으면 null을 반환한다.
				if ((str = bReader.readLine()) == null) {
					System.out.println(
							"----------------------------------파일을 끝까지 읽었습니다-----------------------------------");
					break;
				}

				if (str.startsWith("``0:")) {
					count++;
					if(count % 100000 == 0)
						System.out.println(count);
				}

			}
		} catch (IOException e) {
			System.out.println("countData Exception");
			e.printStackTrace();
		}

		// 파일을 읽은 상태
		System.out.println("데이터의 갯수 : " + count + "건");
		return;
	}

	/*
	 * 데이터 구조가 줄바꿈을 기준으로 정해져있음. 행의 첫 부분에 "``n:" 구조가 발견되었다 -> 컬럼이 끝났으므로 n index의
	 * Max값과 비교 / readline -> 행의 첫 부분에 "``n:" 구조를 발견했다 -> 다음 행도 readline -> \ 행의 첫
	 * 부분이 "``n:" 구조가 아니다 -> 읽은 readline크기를 누적 n index의 max값은 dic구조
	 */

	// 컬럼별 Max 데이터 크기 출력 메소드 -> ` 과 숫자, : 는 1byte , 한글은 3byte
	// 컬럼명 : Max 데이터 구조로 저장 -> dictionary, Map 사용
	private void findMaxDataSizePerColumn() {
		int curr_row = -1; // 현재 체크중인 행의 번호, 0~n까지 , -1은 첫 시작을 구분하기 위한 숫자.
		Map<Integer, Integer> perColumnMaxSize = new HashMap<>();
		int readBuffSize = 0; // 버퍼에 담긴, 읽어온 데이터의 바이트 크기
		boolean flag = false;

		try (BufferedReader bReader = new BufferedReader(createFileReader());) {
			System.out.println("-----------------------------------파일 불러오기-------------------------------------------");
			// 실행
			String str = "";
			int n = 0;
			while ((str = bReader.readLine()) != null) {

				// 컬럼의 시작점을 파악 -> 데이터 형식이 첫 열에 ``n: 구조가 반복되는가?
				if (str.startsWith("``")) {

					// ``n: 에서 n 값을 추출.
					n = findN(str);

					// 파일의 첫 부분이 아니면서 값이 존재한다면
					if (curr_row != -1) {
						perColumnMaxSize.put(curr_row, Math.max(readBuffSize, readBuffSize));
						if (!perColumnMaxSize.containsKey(curr_row)) {
							perColumnMaxSize.put(curr_row, readBuffSize);
						} else {
							if (readBuffSize > perColumnMaxSize.get(curr_row)) {
								perColumnMaxSize.put(curr_row, readBuffSize);
							}
						}
					}

					curr_row = n;
					readBuffSize = str.length() - (str.indexOf(":") + 1);
				} else {
					// 파일의 첫 줄이 아니면서 && 컬럼의 데이터를 아직 읽는중이라면
					if (curr_row != -1) {
						// 그대로 길이 누적.
						readBuffSize += str.length();
					}
				}
			}

			// 파일을 다 읽은 후 그전에 읽어놨던 readBuffSize 저장.
			if (curr_row != -1) {
				if (!perColumnMaxSize.containsKey(curr_row)) {
					perColumnMaxSize.put(curr_row, readBuffSize);
				} else {
					if (readBuffSize > perColumnMaxSize.get(curr_row)) {
						perColumnMaxSize.put(curr_row, readBuffSize);
					}
				}
			}
		} catch (IOException e) {
			System.out.println("findMaxDataSizePerColumn Exception");
			e.printStackTrace();
		} catch (NumberFormatException e) {
			System.out.println("findN(str) Exception");
			e.printStackTrace();
		}

		perColumnMaxSize.forEach((key, value) -> {
			System.out.println(key + " : " + value);
		});
		System.out
				.println("---------------------------------------파일을 끝까지 읽었습니다---------------------------------------");
	}

	private void divideFileByYear() {
		/*
		 * 파일이 다르면 그 파일에 대한 파일 스트림을 생성해야한다, 년도가 같으면 같은 파일 스트림 사용이 가능하다. prevYear 이전 작업
		 * 년도를 기억. 파일 읽기 (inputStream 생성) -> 출원일자 컬럼 구분 -> 년도 추출 -> 년도 구분 -> (prevYear와
		 * 같으면 동일 스트림 사용) -> 이어쓰기 \ (prevYear와 다르면 파일 스트림 생성) -> prevYear보다 이전에 나왔던
		 * Year라면 이어쓰기. \ 이전에 나온적 없는 Year라면 새로 작성
		 */

		/*
		 * 1/14 -> ``0: 컬럼 데이터가 오타, 혹은 누락된 경우 -> ``1: 컬럼의 년도 데이터 이용. -> ``0: 라인을 저장 ->
		 * ``1의 년도 값 추출 ->``1의 데이터가 존재하지않음 -> ``0의 년도가 존재하는가? -> 쓰기작업 -> ``1부터 쓰기작업
		 *
		 * 프로그램은 데이터파일을 돌려서 예외나 오류로 멈춰서는 안된다. OutOfIndex 같은거로 멈추지않도록.
		 */

		Queue<String> existYear = new LinkedList<>(); // BufferedWriter 인스턴스 최대 5개 유지. 각각을 식별할 년도 값 저장 -> bWriters Map에서
														// Key값으로 전달하여 탐색
		Map<String, BufferedWriter> bWriters = new HashMap<>(5);

		String prevYear = "";
		BufferedWriter bWriter = null;

		try (BufferedReader bReader = new BufferedReader(createFileReader());) {
			System.out.println("-----------------------------------파일 불러오기-------------------------------------------");
			// 실행
			String prevStr = null; // ``0: 라인 데이터
			String str = "";

			while ((str = bReader.readLine()) != null) {
				if (str.startsWith("``0:")) {
					prevStr = str;
					continue;
				} else if (str.startsWith("``1:")) {
					String year = null;

					int idx = str.indexOf(":");
					int str_len = str.length();
					int prevStr_len = prevStr.length();

					if (str.length() > 7) {
						year = str.substring(idx + 1, idx + 5);
					} else if (str_len <= 7 && prevStr_len > 7) {
						year = prevStr.substring(idx + 1, idx + 5);
					} else {
						year = "0000";
					}

					System.out.println("추출한 컬럼년도 : " + year);

					// Writer 호출

					// *******Writer의 close()에 주의 : close()는 existYear의 size()가 5개인데 새로운 year 가 들어오면
					// 지워야 하는 year에 맞는 bWriter를 close() 해주는 작업을 반드시 해야한다.*******
					// prevYear와 year 가 다르고 existYear에 year 가 없고 아직 existYear가 5개가 아니라면
					if (!year.equals(prevYear) && !existYear.contains(year) && existYear.size() != 5) {
						// 년도 추가
						existYear.add(year);

						// Writer 생성
						bWriter = new BufferedWriter(reWriteFileWriter(year, true));

						// Writer를 HashMap 에 "year : bWriter"의 key-value 쌍 저장
						bWriters.put(year, bWriter);
						// ``0: 데이터 저장
						bWriter.write(prevStr);
						bWriter.newLine();
						prevStr = null;
						// ``1: 데이터 저장
						bWriter.write(str);
						bWriter.newLine();
						bWriter.flush();
						// 작업 년도 갱신
						prevYear = year;
					} else if (!year.equals(prevYear) && existYear.contains(year)) {
						// prevYear와 다르다 && existYear에 있다. == 생성된 bWriter가 존재하니 bWriter를 HashMap에서 year를
						// key로 찾는다.

						// bWriter를 가져옴 (인스턴스 생성이 아님) -> 마찬가지로 이전에 작업했던 bWriter 역시 close() 하지않는다.
						bWriter = bWriters.get(year);
						// ``0: 데이터 저장
						bWriter.write(prevStr);
						bWriter.newLine();
						prevStr = null;
						// ``1: 데이터 저장
						bWriter.write(str);
						bWriter.newLine();
						bWriter.flush();
						// 작업 년도 갱신
						prevYear = year;

					} else if (year.equals(prevYear)) {
						// prevYear와 같다 = existYear에 year가 있는거나 마찬가지, 그리고 그 상황은 existYear.size()를 체크할
						// 필요도 없다
						// 새로운 데이터를 찾은상황 -> 그대로 사용중인 bWriter에 작성
						if (prevStr != null) {
							bWriter.write(prevStr);
							bWriter.newLine();
							prevStr = null;
						}
						bWriter.write(str);
						bWriter.newLine();
						bWriter.flush();

					} else {
						// 새로운 year 값을 받았는데 existYear.size() == 5 인 case
						// existYear배열의 마지막 index에 year 입력
						// Queue 에서 맨 앞의 값을 poll() 해서 year값 반환 -> Queue의 맨 뒤에 year를 add() -> 가장 오래 안 쓰인
						// year을 제거

						// FIXME Queue를 사용하면안된다, 중복값이 다시 발생, 가장 오래된 값 먼저 제거 하면서 중복을 제거하는 방법 찾아야함.
						String oldYear = existYear.poll();
						existYear.add(year);

						// bWriters의 스트림은 close()
						bWriters.get(oldYear).close();

						// 새로운 bWriters 인스턴스 생성
						bWriter = new BufferedWriter(reWriteFileWriter(year, true));

						// 쓰기 실행
						// ``0: 데이터 저장
						bWriter.write(prevStr);
						bWriter.newLine();
						prevStr = null;
						// ``1: 데이터 저장
						bWriter.write(str);
						bWriter.newLine();
						bWriter.flush();

						// HashMap old값 삭제
						bWriters.get(oldYear).close();
						bWriters.remove(oldYear);

						// HashMap new값 저장
						bWriters.put(year, bWriter);
					}

				}
				// 이외의 case는 그냥 이어 쓰기, 모두 동일 컬럼의 데이터
				bWriter.write(str);
				bWriter.newLine();
				bWriter.flush();
			}

//			while ((str = bReader.readLine()) != null) {
//
//				// 출원일자 컬럼 구분
//				if (str.startsWith("``0:")) {
//					// 이전 문자열 보관 -> 재 loop
//					prevStr = str;
//					continue;
//				} else if (str.startsWith("``1:")) {
//
//					// ``0: 데이터도 없고, ``1: 데이터도 없다면 따로 파일로 내보내자.
//					// 현재 문제가 가변 길이 문자열의 경계 오버플로우 이슈. -> 요청에 응답할 만큼의 문자열이 있는가를 확인해야한다.
//					// 만약 0도 1도 없다면?? -> year를 0000 으로 반환??
//					String year = null;
//
//					// 반복 사용으로 인한 불필요한 코드 및 부하 방지
//					int idx = str.indexOf(":");
//					int str_len = str.length();
//					int prevStr_len = prevStr.length();
//
//					if(str.length() > 7) {
//						year = str.substring(idx+1,idx+5);
//					}else if(str_len <= 7  && prevStr_len > 7) {
//						year = prevStr.substring(idx+1, idx+5);
//					}else {
//						year = "0000";
//					}
//
//					//String year = str.length() > 8 ? str.substring(4, 8) : prevStr.substring(4, 8);
//
//					System.out.println("추출한 컬럼년도 : " + year);
//
//					// prevYear와 year가 다르다 && existYear에 없다
//					if (!year.equals(prevYear) && !existYear.contains(year)) {
//						// 맨 첫 실행 상태가 아니다 -> bWriter close()
//						if (bWriter != null) {
//							bWriter.close();
//						}
//						// 새로운 출력 스트림 생성
//						bWriter = new BufferedWriter(createFileWriter(year));
//						// ``0: 데이터 저장
//						bWriter.write(prevStr);
//						bWriter.newLine();
//						prevStr = null;
//						// ``1: 데이터 저장
//						bWriter.write(str);
//						bWriter.newLine();
//						bWriter.flush();
//						existYear.add(year);
//						prevYear = year;
//					} else if (!year.equals(prevYear) && existYear.contains(year)) {
//						// prevYear와 다르다 && existYear에 있다. && 생성된 bWriter가 존재한다.
//						// 새로운 출력 스트림 생성 but, 쓰던 파일에 이어쓰기
//						bWriter.close();
//						bWriter = new BufferedWriter(reWriteFileWriter(year, true)); // 이어쓰기 모드
//						// ``0: 데이터 저장
//						bWriter.write(prevStr);
//						bWriter.newLine();
//						prevStr = null;
//						// ``1: 데이터 저장
//						bWriter.write(str);
//						bWriter.newLine();
//						bWriter.flush();
//						prevYear = year;
//					} else {
//						// prevYear와 같으면 동일 스트림 사용 -> 이어쓰기
//						if (prevStr != null) {
//							bWriter.write(prevStr);
//							bWriter.newLine();
//							prevStr = null;
//						}
//						bWriter.write(str);
//						bWriter.newLine();
//						bWriter.flush();
//					}
//				} else {
//					// 나머지는 모두 동일 컬럼의 데이터
//					bWriter.write(str);
//					bWriter.newLine();
//					bWriter.flush();
//				}
//			}
			// 파일의 마지막 부분 혹시나 있을 데이터를 출력.
			bWriter.newLine();
			bWriter.flush();
			System.out.println(
					"---------------------------------------파일을 끝까지 읽었습니다---------------------------------------");
			Iterator iter = existYear.iterator();
			while (iter.hasNext()) {
				System.out.print(iter.next() + " "); // 1 2 3
			}
			System.out.println();
		} catch (IOException e) {
			System.out.println("divideFileByYear Exception");
			e.printStackTrace();
		} finally {
			// 생성했던 다중 인스턴스 close() 호출 ** 필수 **
			int n = 1;
			for (BufferedWriter bW : bWriters.values()) {
				if (bW != null) {
					try {
						bW.close();
						System.out.println(n + "번째 BufferedWriter close() 완료");
						n++;
					} catch (IOException e) {
						System.out.println("BufferedWriter close() 중 발생");
						e.printStackTrace();
					}
				}
			}
		}
	}

	private int findN(String str) throws NumberFormatException {
		// TODO Auto-generated method stub
//		System.out.println("findN 실행");
		int N = Integer.parseInt(str.substring(2, str.indexOf(':')));
//		System.out.println("찾은 N 값 : " + N);

		return N;
	}
}
