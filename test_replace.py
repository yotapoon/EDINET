# -*- coding: utf-8 -*-

if __name__ == "__main__":
    print("--- Zero-Base Replace Function Verification ---")

    # 検証対象の文字列を直接定義
    test_string = '凸版印刷㈱'

    # 置換処理を実行
    cleaned_string = test_string.replace('㈱', '')

    # 結果を比較
    print(f"Original string : [ {test_string} ]")
    print(f"Cleaned string  : [ {cleaned_string} ]")

    print("\n--- Conclusion ---")
    if test_string != cleaned_string and len(cleaned_string) == 4:
        print("SUCCESS: The .replace() function is working correctly in isolation.")
        print("This strongly suggests the problem occurs during data loading from the database or within the Pandas DataFrame processing.")
    else:
        print("FAILURE: The .replace() function is NOT working as expected, even in the simplest case.")
        print("This indicates a deep, unresolvable issue with the Python execution environment.")

    print("\n--- Verification finished ---")

