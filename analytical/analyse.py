import pandas as pd


def main():
    df = pd.read_csv('results/before_index/Q1_V1.csv')
    print(df['EXECUTION_TIME'].mean())


if __name__ == "__main__":
    main()
