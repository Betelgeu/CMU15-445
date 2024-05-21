import os
import time
import subprocess
import re


def sort_key(filename):
    # This function extracts the question number from the filename and uses it for sorting
    match = re.search(r"q(\d+)_", filename)
    if match:
        return int(match.group(1))
    return 0

def get_duckdb_filename(filename):
    # 将文件名拆分为名称和扩展名
    name, extension = os.path.splitext(filename)
    dot_index = filename.find('.')
    filename = filename[:dot_index] + ".duckdb" + extension
    return filename

def main():
    if not os.path.exists("ans"):
        print("cannot find ans folder")
        return

    files = sorted(
        [f for f in os.listdir() if f.endswith(".sql") and f != "import.sql"],
        key=sort_key,
    )
    scores = {
        "q1_sample.duckdb.sql": 0,
        "q2_beatles_uk_releases.duckdb.sql": 5,
        "q2_beatles_uk_releases.sqlite.sql": 5,
        "q3_new_wine_in_old_bottles.duckdb.sql": 5,
        "q3_new_wine_in_old_bottles.sqlite.sql": 5,
        "q4_devil_in_the_details.duckdb.sql": 5,
        "q4_devil_in_the_details.sqlite.sql": 5,
        "q5_elvis_best_month.duckdb.sql": 10,
        "q5_elvis_best_month.sqlite.sql": 10,
        "q6_us_artist_groups_per_decade.duckdb.sql": 10,
        "q6_us_artist_groups_per_decade.sqlite.sql": 10,
        "q7_pso_friends.duckdb.sql": 10,
        "q7_pso_friends.sqlite.sql": 10,
        "q8_john_not_john.duckdb.sql": 10,
        "q8_john_not_john.sqlite.sql": 10,
        "q9_music_in_the_world.duckdb.sql": 15,
        "q9_music_in_the_world.sqlite.sql": 15,
        "q10_latest_releases.duckdb.sql": 15,
        "q10_latest_releases.sqlite.sql": 15,
    }
    total_score = 0
    failed_files = []

    for file in files:
        corresponding_ans_file = "ans/" + file
        print("try to analysing file: " + file + "with answer file: " + corresponding_ans_file)
        if not os.path.exists(corresponding_ans_file):
            print("cannot find corresponding answer file for " + file)
            duck_filename = "ans/" + get_duckdb_filename(file)
            if(file.endswith('.sqlite.sql') and os.path.exists(duck_filename)):
                print("instead, found corresponding duckdb file: " + duck_filename)
                corresponding_ans_file = "ans/" + duck_filename
            else:
                print("cannot find corresponding duckdb file: " + duck_filename +" for " + file)
                continue

        print("*******************************")
        print("testing {}".format(file))
        start_time = time.perf_counter()

        # test command
        if file.endswith(".duckdb.sql"):
            db_command = "./duckdb musicbrainz-cmudb2023.duckdb"
        elif file.endswith(".sqlite.sql"):
            db_command = "sqlite3 musicbrainz-cmudb2023.db"
        else:
            print("Unknown file type for {}".format(file))
            continue
        # ans command
        if corresponding_ans_file.endswith(".duckdb.sql"):
            ans_db_command = "./duckdb musicbrainz-cmudb2023.duckdb"
        elif corresponding_ans_file.endswith(".sqlite.sql"):
            ans_db_command = "sqlite3 musicbrainz-cmudb2023.db"
        else:
            print("Unknown file type for {}".format(corresponding_ans_file))
            continue

        command = [
            "bash",
            "-c",
            f'diff <(echo ".read {corresponding_ans_file}" | {db_command}) <(echo ".read {file}" | {ans_db_command})',
        ]

        try:
            process = subprocess.Popen(
                command, stdout=subprocess.PIPE, stderr=subprocess.PIPE
            )
            stdout, stderr = process.communicate()
            if process.returncode != 0:
                print("Differences found for:", file)
                print(stdout.decode())
                failed_files.append(file)
            else:
                print(f"{file} passed!")
                total_score += scores.get(file, 0)

        except Exception as e:
            print("Call of {} failed: {}".format(" ".join(command), e))
            return

        elapsed_time = time.perf_counter() - start_time
        print(f"spent {elapsed_time:0.4f} seconds")
        print("*******************************")
        print()

    print("\nFinal score:", total_score)
    if total_score != 100:
        print("Files that did not pass:")
        for failed_file in failed_files:
            print(failed_file)


if __name__ == "__main__":
    main()
