import os
with open("build.sbt") as f:
    # print(f.readlines())
    lines: list[str] = [x for x in f.readlines() if x.startswith("ThisBuild")]
    versionJar: str = lines[0].split(":=")[1].strip().strip('"')
    versionScala: str = ".".join(lines[1].split(":=")[1].strip().strip('"').split(".")[:2])
    print(versionJar, versionScala, sep="\n")
