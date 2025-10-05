import pandas as pd
import json
import re

from neo4j import GraphDatabase
import pandas as pd

def Netflix():
    print("Loading Netflix-data...")
    print()

    MAX_USER = 1000  # 这里选定读取一千个用户
    d_movie = dict()
    s_movie = set()

    # ====================================================================================== 生成 movies.csv数据集（
    out_movies = open("out_movies.csv", "w")
    out_movies.write("title\n")

    for line in open("movie_titles.csv", "r", encoding='ISO-8859-1'):
        line = line.strip().split(',')  # 通过“,”字符进行切割
        movie_id = int(line[0])
        title = line[2].replace("\"", "")  # 将“\”字符去掉
        title = "\"" + title + "\""

        d_movie[movie_id] = title

        if title in s_movie:
            continue
        s_movie.add(title)

        out_movies.write(f"{title}\n")  # 读入数据

    print("out_movies.csv Create Success...")
    out_movies.close()  # 关闭

    # ====================================================================================== 生成 grade数据集
    out_grade = open("out_grade.csv", "w")
    out_grade.write("user_id,title,grade\n")

    files = ["combined_data_1.txt"]
    for f in files:
        movie_id = -1
        for line in open(f, "r"):
            pos = line.find(":")
            if pos != -1:
                movie_id = int(line[:pos])
                continue
            line = line.strip().split(",")
            user_id = int(line[0])  # 用户编号
            rating = int(line[1])  # 评分

            if user_id > MAX_USER:  # 获取1000个用户（看自己需求）
                continue

            out_grade.write(f"{user_id},{d_movie[movie_id]},{rating}\n")

    print("out_greade.csv Create Success...")
    out_grade.close()


# =======================================================================================================
"""
genre.csv数据集（电影类型）
keyword.csv数据集（电影关键词）
productor.csv数据集（电影导演及公司）
"""


def TMDB():
    print("Loading TMDB-data...")
    print()

    # ============================================================== 写入表格列名标题
    pattern = re.compile("[A-Za-z0-9]+")
    out_genre = open("out_genre.csv", "w", encoding="utf-8")
    out_genre.write("title,genre\n")
    out_keyword = open("out_keyword.csv", "w", encoding="utf-8")
    out_keyword.write("title,keyword\n")
    out_productor = open("out_productor.csv", "w", encoding="utf-8")
    out_productor.write("title,productor\n")

    # ============================================================== 读入数据
    df = pd.read_csv("tmdb_5000_movies.csv", sep=",")
    json_columns = ['genres', 'keywords', 'production_companies']
    for column in json_columns:
        df[column] = df[column].apply(json.loads)  # 处理字典
    df = df[["genres", "keywords", "original_title", "production_companies"]]

    for _, row in df.iterrows():
        title = row["original_title"]
        if not pattern.fullmatch(title):  # 匹配
            continue
        title = "\"" + title + "\""

        # ====================================================================================== 生成out_genre.csv数据集
        for g in row["genres"]:
            genre = g["name"]
            genre = "\"" + genre + "\""
            out_genre.write(f"{title},{genre}\n")

        # ====================================================================================== 生成out_keyword数据集
        for g in row["keywords"]:
            keyword = g["name"]
            keyword = "\"" + keyword + "\""
            out_keyword.write(f"{title},{keyword}\n")

        # ====================================================================================== 生成out_productor.csv数据集
        for g in row["production_companies"]:
            productor = g["name"]
            productor = "\"" + productor + "\""
            out_productor.write(f"{title},{productor}\n")

    print("out_genre.csv Create Success...")
    print("out_keyword.csv Create Success...")
    print("out_productor.csv Create Success...")
    out_genre.close()
    out_keyword.close()
    out_productor.close()

# =======================================================================================================
if __name__ == "__main__":
    Netflix()
    print("=" * 40)
    TMDB()

# 连接Neo4j数据库
uri = "neo4j://localhost:7687"  # 本地地址
driver = GraphDatabase.driver(uri, auth=("neo4j", "zwh20030223p"))  # 自己的账号和密码

# 相关设置及参数
k = 10
movies_common = 3
user_common = 2
threshold_sim = 0.9


def load_data():
    with driver.session() as session:
        session.run("""MATCH ()-[r]->() DELETE r""")
        session.run("""MATCH (r) DELETE r""")

        # ================================================================================ 创建电影名称数据实体图
        print("Loading movies...")

        session.run("""
            LOAD CSV WITH HEADERS FROM "file:///out_movies.csv" AS csv
            CREATE (:Movie {title: csv.title})
        """)

        # ================================================================================ 创建用户评分实体图
        print("Loading gradings...")

        session.run("""
            LOAD CSV WITH HEADERS FROM "file:///out_grade.csv" AS csv
            MERGE (m:Movie {title: csv.title})
            MERGE (u:User {id: toInteger(csv.user_id)})
            CREATE (u)-[:RATED {grading : toInteger(csv.grade)}]->(m)
        """)

        # ================================================================================ 创建电影类型实体图
        print("Loading genres...")

        session.run("""
            LOAD CSV WITH HEADERS FROM "file:///out_genre.csv" AS csv
            MERGE (m:Movie {title: csv.title})
            MERGE (g:Genre {genre: csv.genre})
            CREATE (m)-[:HAS_GENRE]->(g)
        """)

        # ================================================================================ 创建电影关键词实体图
        print("Loading keywords...")

        session.run("""
            LOAD CSV WITH HEADERS FROM "file:///out_keyword.csv" AS csv
            MERGE (m:Movie {title: csv.title})
            MERGE (k:Keyword {keyword: csv.keyword})
            CREATE (m)-[:HAS_KEYWORD]->(k)
        """)

        # ================================================================================ 创建电影导演、公司等实体图
        print("Loading productors...")

        session.run("""
            LOAD CSV WITH HEADERS FROM "file:///out_productor.csv" AS csv
            MERGE (m:Movie {title: csv.title})
            MERGE (p:Productor {name: csv.productor})
            CREATE (m)-[:HAS_PRODUCTOR]->(p)
        """)


# ================================================================================ 主函数

if __name__ == "__main__":
    if int(input("是否需要重新加载并创建知识图谱？（输入0或1）")):
        load_data()
        print("创建完毕...")


def queries():
    while True:
        userid = input("请输入要为哪位用户推荐电影，输入其ID即可（回车结束）：")

        if userid == "":
            break

        userid = int(userid)
        m = int(input("为该用户推荐多少个电影呢？"))

        genres = []
        if int(input("是否需要过滤不喜欢的类型？（输入0或1）")):
            with driver.session() as session:
                try:
                    q = session.run(f"""MATCH (g:Genre) RETURN g.genre AS genre""")
                    result = []
                    for i, r in enumerate(q):
                        result.append(r["genre"])
                    df = pd.DataFrame(result, columns={"genre"})
                    print()
                    print(df)

                    inp = input("输入不喜欢的类型索引即可，例如：1 2 3")
                    if len(inp) != 0:
                        inp = inp.split(" ")
                        genres = [df["genre"].iloc[int(x)] for x in inp]
                except:
                    print("Error")

        # 找到当前ID
        with driver.session() as session:
            q = session.run(f"""
                    MATCH (u1:User {{id : {userid}}})-[r:RATED]-(m:Movie)
                    RETURN m.title AS title, r.grading AS grade
                    ORDER BY grade DESC
                """)

            print()
            print("Your ratings are the following（你的评分如下）:")

            result = []
            for r in q:
                result.append([r["title"], r["grade"]])

            if len(result) == 0:
                print("No ratings found")
            else:
                df = pd.DataFrame(result, columns=["title", "grade"])
                print()
                print(df.to_string(index=False))
            print()

            session.run(f"""
                MATCH (u1:User)-[s:SIMILARITY]-(u2:User)
                DELETE s
            """)

            # 找到当前用户评分的电影以及这些电影被其他用户评分的用户，with是吧查询集合当做结果方便后面用where余弦相似度计算
            """
            Cosine相似度计算法(Cosine Similarity)
            """
            session.run(f"""
                    MATCH (u1:User {{id : {userid}}})-[r1:RATED]-(m:Movie)-[r2:RATED]-(u2:User)
                    WITH
                        u1, u2,
                        COUNT(m) AS movies_common,
                        SUM(r1.grading * r2.grading)/(SQRT(SUM(r1.grading^2)) * SQRT(SUM(r2.grading^2))) AS sim
                    WHERE movies_common >= {movies_common} AND sim > {threshold_sim}
                    MERGE (u1)-[s:SIMILARITY]-(u2)
                    SET s.sim = sim
            """)

            # 过滤操作
            Q_GENRE = ""
            if (len(genres) > 0):
                Q_GENRE = "AND ((SIZE(gen) > 0) AND a"
                Q_GENRE += "(ANY(x IN " + str(genres) + " WHERE x in gen))"
                Q_GENRE += ")"

            # 找到相似的用户，然后看他们喜欢什么电影Collect，将所有值收集到一个集合List中
            """
            s:SIMILARITY通过关系边查询
            ORDER BY 降序排列
            """
            q = session.run(f"""
                    MATCH (u1:User {{id : {userid}}})-[s:SIMILARITY]-(u2:User)
                    WITH u1, u2, s
                    ORDER BY s.sim DESC LIMIT {k}
                    MATCH (m:Movie)-[r:RATED]-(u2)
                    OPTIONAL MATCH (g:Genre)--(m)
                    WITH u1, u2, s, m, r, COLLECT(DISTINCT g.genre) AS gen
                    WHERE NOT((m)-[:RATED]-(u1)) {Q_GENRE}
                    WITH
                        m.title AS title,
                        SUM(r.grading * s.sim)/SUM(s.sim) AS grade,
                        COUNT(u2) AS num,
                        gen
                    WHERE num >= {user_common}
                    RETURN title, grade, num, gen
                    ORDER BY grade DESC, num DESC
                    LIMIT {m}
            """)

            print("Recommended movies（推荐电影如下）:")

            result = []
            for r in q:
                result.append([r["title"], r["grade"], r["num"], r["gen"]])
            if len(result) == 0:
                print("No recommendations found（没有找到适合推荐的）")
                print()
                continue

            df = pd.DataFrame(result, columns=["title", "avg grade", "num recommenders", "genres"])
            print()
            print(df.to_string(index=False))
            print()

if __name__ == "__main__":
    queries()


