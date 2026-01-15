import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

//TIP 코드를 <b>실행</b>하려면 <shortcut actionId="Run"/>을(를) 누르거나
// 에디터 여백에 있는 <icon src="AllIcons.Actions.Execute"/> 아이콘을 클릭하세요.
public class Feedback {

	/*
	 * 인수인계 사수분이 빠르게 작성해본 방식. StringBuilder, 과하지않은 if 로직, 아스키코드 활용.
	 *
	 */

    public static void main(String[] args) {
        Main main = new Main();
        String fileName = "./n.kor_4.dat";
//        sizeCounter(fileName, main);
        splitFile(fileName);

    }

    private static void sizeCounter(String fileName, Main main) {
        try (BufferedReader br = new BufferedReader(new FileReader(fileName))){
            System.out.println("start test");
            main.readAndCount(fileName);
            System.out.println("start end");


        } catch (Exception e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }
    }

    private static void splitFile(String input) {
        try (BufferedReader br = new BufferedReader(new FileReader(input))){
            String line;
            List<String> temp = new ArrayList<>();
            StringBuilder sb = new StringBuilder();

            while ((line = br.readLine()) != null) {
                // ``시작라인이면 이전 데이터(sb) 존재 유무 확인
                if (line.startsWith("``")) {
                    if (!sb.isEmpty()) {
                        temp.add(sb.toString());
                        // StringBuilder 초기화 null로하면 NullPointerException
                        sb.setLength(0);
                    }
                }
                //1건 처리
                if (line.startsWith("``0") && !temp.isEmpty()) {
                    String year = getYear(temp);
                    //bw 열기 or 기존 bw 있는지 확인



                }
                sb.append(line);
                sb.append(System.lineSeparator());
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    private static int[] yearColIdx = new int[] {0, 1};
    private static String getYear(List<String> lines) {
        String year = "0000";
        for (int colIdx : yearColIdx) {
            String data = lines.get(colIdx);
            char startC = data.charAt(data.indexOf(':') + 1);
            if (startC >= 48 && startC <= 57) {
                int beginIndex = data.indexOf(':') + 1;
                year = data.substring(beginIndex, beginIndex + 4);
                break;
            }
        }
        return year;
    }

    private void readAndCount(String input) {
        Map<String, Integer> sizeMap = new HashMap<>();
        int len = 0;
        String n = "";
        String prevN = "";
        StringBuilder sb = new StringBuilder();
        try (BufferedReader br = new BufferedReader(new FileReader(input))){
            String line;
            while ((line = br.readLine()) != null) {
                // 컬럼 라인
                if (line.startsWith("``")) {
                    // 컬럼 번호 추출
                    n = line.substring(2, line.indexOf(":"));

                    // 컬럼 번호가 바뀔 경우(새로운 컬럼 라인) 사이즈 map 데이터 업데이트
                    if (!prevN.equals("") && !n.equals(prevN)) {
                        int size = sizeMap.getOrDefault(prevN, 0);
                        if (size < len){
                            sizeMap.put(prevN, len);
                            len = 0;

                            System.out.println("----------------------------------------------------------");
                            System.out.println(prevN);
                            System.out.println(sb.toString());
                            System.out.println("len = " + sb.toString().length());
                            sb = new StringBuilder();
                        }
                    }

                    len = line.length() - (line.indexOf(':') + 1);
                    len += System.lineSeparator().length();

                    sb.append(line.substring(line.indexOf(':') + 1));
                    sb.append(System.lineSeparator());
                }
                // 컬럼 라인이 아닌 라인(컬럼의 다음 데이터 라인)
                else {
                    len += line.length();
                    len += System.lineSeparator().length();

                    sb.append(line);
                    sb.append(System.lineSeparator());
                }

                prevN = n;
            }
            // 마지막 데이터 update
            if (len > 0) {
                int size = sizeMap.getOrDefault(prevN, 0);
                if (size < len){
                    sizeMap.put(prevN, len);
                }
            }

            // 출력
            for (String colNo : sizeMap.keySet()) {
                System.out.println(colNo + " : " + sizeMap.get(colNo));
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

//    private void readTest(BufferedReader br) throws Exception {
//        Map<String, Integer> lenMap = new HashMap<>();
//        String line;
//        int len = 0;
//        String nStr = "";
//        String prevNStr = "";
//        while((line = br.readLine()) != null) {
//            // 컬럼 라인
//            if (line.startsWith("``")) {
//                nStr = line.substring(2, line.indexOf(':'));
//                if (len > 0) {
//                    int cnt = lenMap.getOrDefault(nStr, 0);
//                    if (cnt < len) {
//                        lenMap.put(nStr, len);
//
//                        // 사이즈 초기화
//                        len = 0;
//                    }
//                }
//                // 사이즈 체크
//                len = line.length() - (line.indexOf(":") + 1);
//
//            }
//            // 컬럼 라인이 아닌 라인(이전 컬럼의 다음 개행)
//            else {
//                len += line.length();
//            }
//
//
//        }
//        if (len > 0 && !nStr.equals("")) {
//            int cnt = lenMap.getOrDefault(nStr, 0);
//            if (cnt < len) {
//                lenMap.put(nStr, len);
//            }
//
//        }
//
//        for (String key : lenMap.keySet()) {
//            System.out.println(key + " = " + lenMap.get(key));
//        }
//
//    }
//
//    private String findNStr(String str) {
//        for (int i = 2; i < str.length(); i++) {
//            if (str.charAt(i) != ':') {
//                return str.substring(2, i);
//            }
//        }
//        return null;
//    }
}