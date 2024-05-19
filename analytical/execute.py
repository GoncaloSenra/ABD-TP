
import sys
import time

import psycopg2

def parse_time(s: str) -> float:
    return float(s.split(":")[1].strip().split(" ")[0])

def execute_query(queries: dict[str, list[str]]):
    results = []

    try:
        # Connect to the PostgreSQL database
        connection = psycopg2.connect(
            user="postgres",
            password="postgres",
            host="localhost",
            port="5432",
            database="abd_stack_overflow"
        )

        # Create a cursor object
        cursor = connection.cursor()

        for k in queries:
            count = 1
            for q in queries[k]:
                print(f"Executing {k}, iteration {count}")
                count += 1
                # Execute the query
                cursor.execute(q)
                # Fetch all the rows
                rows = cursor.fetchall()
                # Print the rows
                result = ""
                planning_time = 0
                execution_time = 0

                for row in rows:
                    r = row[0]
                    if str(r).startswith("Planning Time: "):
                        planning_time = parse_time(str(r))
                    elif str(r).startswith("Execution Time: "):
                        execution_time = parse_time(str(r))
                    result += r + "\n"

                results.append((k, planning_time, execution_time, len(rows), result))

        return results

    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to PostgreSQL", error)

    finally:
        # Close the cursor and connection
        if connection:
            cursor.close()
            connection.close()
            print("PostgreSQL connection is closed")

# Params:
# max_iter
# output file
# query files
# exemplo:
# python .\execute.py 30 results\q1_owneruserid_idxs_2.csv Q1 Q1_V1 Q1_V2
if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("No files passed")
    else:
        max_iter = int(sys.argv[1])
        output_file = sys.argv[2]
        query_files = sys.argv[3:]

        queries = {}

        for f in query_files:
            queries[f] = []
            filename = f if f.lower().endswith(".sql") else f + ".sql"
            with open(filename, 'r') as file:
                query = file.read()
                query = query if query.lower().startswith("explain analy") else "explain analyze " + query
                for i in range(max_iter):
                    queries[f].append(query)

        t = time.time()
        res = execute_query(queries)
        print(f'Run time: {time.time() - t}')

        content = "QUERY,PLANNING_TIME,EXECUTION_TIME,ROWS\n"
        for r in res:
            content += f"{r[0]},{r[1]},{r[2]},{r[3]}\n"

        with open(output_file, 'w') as f:
            f.write(content)

        print(f"Results writed to {output_file}")
