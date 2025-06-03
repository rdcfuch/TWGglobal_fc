MATCH p=(d:Deal)-[h:HASPROPERTY]->(prop:Property)-[l:LOCATEDAT]->(addr) RETURN p LIMIT 25

MATCH p=(d:Deal)-[h:HASPROPERTY]->(prop:Property)-[l:INMSA]->(addr) RETURN p LIMIT 25

MATCH p=(d:Deal)-[h:HASPROPERTY]->(prop:Property)-[l:LOCATEDAT]->(addr) OPTIONAL MATCH (prop)-[o:BUILTAT]->(y) RETURN p,y LIMIT 25


MATCH p=(d:Deal)-[h:HASPROPERTY]->(prop:Property)-[l:LOCATEDAT]->(addr) OPTIONAL MATCH (prop)-[o:INMSA]->(y) RETURN p,y LIMIT 25


