import sqlite3
import csv
import os

파일이름 = "pm_comp_info"
#pm_comp_info
#bm_history_lk
#TPSS_기준정보

DB_PATH = f"/config/work/sharedworkspace/db/{파일이름}.db"

# 변환된 CSV 저장 경로 (원본 DB 파일과 같은 폴더)
OUTPUT_DIR = os.path.dirname(DB_PATH)  # "/config/work/sharedworkspace/db/"
BASE_FILENAME = os.path.splitext(os.path.basename(DB_PATH))[0]  # "bm_history_lk"

def export_all_tables(db_path, output_dir, base_filename):
    """DB의 모든 테이블을 찾아서 원본 파일명.csv로 변환 (한글 깨짐 방지)"""
    try:
        # 데이터베이스 연결
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # 모든 테이블 목록 가져오기
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = [t[0] for t in cursor.fetchall()]

        if not tables:
            print("❌ 변환할 테이블이 없습니다.")
            return

        print(f"📌 총 {len(tables)}개의 테이블을 찾았습니다. 변환을 시작합니다...")

        # 변환된 파일 경로 (원본 DB 파일 이름 + .csv)
        output_csv_path = os.path.join(output_dir, f"{base_filename}.csv")

        with open(output_csv_path, "w", newline="", encoding="utf-8-sig") as f:  # ✅ 한글 깨짐 방지
            writer = csv.writer(f)

            for table_name in tables:
                # 테이블 데이터 가져오기
                cursor.execute(f"SELECT * FROM {table_name}")
                rows = cursor.fetchall()

                # 컬럼명 가져오기
                col_names = [desc[0] for desc in cursor.description]

                # 테이블 이름을 구분자로 추가
                writer.writerow([f"-- {table_name} --"])  # 테이블 구분선
                writer.writerow(col_names)  # 컬럼명 저장
                writer.writerows(rows)  # 데이터 저장
                writer.writerow([])  # 테이블 간 공백 추가

                print(f"✅ {table_name} 변환 완료")

        print(f"\n🎉 모든 테이블 변환 완료! 파일 저장: {output_csv_path}")

    except Exception as e:
        print(f"❌ 오류 발생: {e}")

    finally:
        # 연결 종료
        conn.close()

# 실행
export_all_tables(DB_PATH, OUTPUT_DIR, BASE_FILENAME)
