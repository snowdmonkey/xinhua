import neo4j


driver = neo4j.GraphDatabase.driver("neo4j://localhost:7687", auth=("neo4j", "2much4ME"))


f = open("./data/triplets.txt", "w")

with driver.session() as session:
    cql = "MATCH (h)-[r]->(t) RETURN h, r, t"
    results = session.run(cql)
    for i, record in enumerate(results):
        h, r, t = record
        h_str = next(iter(h.labels)) + "/" + (h.get("id") if h.get("id") is not None else h.get("name"))
        r_str = r.type
        t_str = next(iter(t.labels)) + "/" + (t.get("id") if t.get("id") is not None else t.get("name"))
        f.write("\t".join([h_str, r_str, t_str]))
        f.write("\n")

        if i % 1_000 == 0:
            print(i)

f.close()
driver.close()
